package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"time"

	// Using the one library that is available
	"github.com/lukpank/go-glpk/glpk"
)

// --- 1. JSON Data Structures ---

type InputData struct {
	RaceStartUTC        string                  `json:"raceStartUTC"`
	DurationHours       int                     `json:"durationHours"`
	AvgLapTimeInSeconds float64                 `json:"avgLapTimeInSeconds"`
	PitTimeInSeconds    float64                 `json:"pitTimeInSeconds"`
	FuelTankSize        float64                 `json:"fuelTankSize"`
	FuelUsePerLap       float64                 `json:"fuelUsePerLap"`
	FirstStintDriver    string                  `json:"firstStintDriver,omitempty"`
	TeamMembers         []TeamMember            `json:"teamMembers"`
	Availability        map[string]Availability `json:"availability"`
}

type TeamMember struct {
	Name             string `json:"name"`
	IsDriver         bool   `json:"isDriver"`
	IsSpotter        bool   `json:"isSpotter"`
	PreferredStints  int    `json:"preferredStints"`
	Timezone         int    `json:"timezone"`
	MinimumRestHours int    `json:"minimumRestHours,omitempty"`
}

type Availability map[string]string

// --- 2. Output Data Structures ---

type OutputData struct {
	RaceData             InputData       `json:"raceData"`
	Schedule             []ScheduleEntry `json:"schedule"`
	SolveDurationSeconds float64         `json:"solveDurationSeconds"`
}

type ScheduleEntry struct {
	Stint   int    `json:"stint"`
	Driver  string `json:"driver"`
	Spotter string `json:"spotter,omitempty"`
}

// --- 3. Main Application Logic ---

func main() {
	timeLimitSec := 30
	quiet := false

	setupLogging(quiet)

	var inputFile *os.File
	var err error

	if len(os.Args) > 1 {
		filePath := os.Args[1]
		log.Printf("Reading input data from file: %s", filePath)
		inputFile, err = os.Open(filePath)
		if err != nil {
			log.Fatalf("Failed to open input file: %v", err)
		}
		defer inputFile.Close()
	} else {
		log.Println("Reading input data from stdin...")
		inputFile = os.Stdin
	}

	data, err := loadInputData(inputFile)
	if err != nil {
		log.Fatalf("Failed to load data: %v", err)
	}

	startTime := time.Now()

	schedule, err := solveSchedule(data, timeLimitSec)

	solveDuration := time.Since(startTime).Seconds()

	if err != nil {
		log.Fatalf("Solver failed: %v", err)
	}
	log.Printf("Total solver time: %.2f seconds.", solveDuration)

	outputData := OutputData{
		RaceData:             *data,
		Schedule:             schedule,
		SolveDurationSeconds: solveDuration,
	}

	outputWriter := os.Stdout
	log.Println("Writing schedule to stdout.")

	encoder := json.NewEncoder(outputWriter)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(outputData); err != nil {
		log.Fatalf("Failed to write output JSON: %v", err)
	}
	log.Println("Solver finished successfully.")
}

func setupLogging(quiet bool) {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)
	if quiet {
		log.SetOutput(io.Discard)
	}
}

func loadInputData(reader io.Reader) (*InputData, error) {
	bytes, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to read input: %w", err)
	}

	var data InputData
	if err := json.Unmarshal(bytes, &data); err != nil {
		return nil, fmt.Errorf("failed to parse JSON: %w", err)
	}
	return &data, nil
}

// --- 4. The Core Solver Function (GLPK Version) ---

// Helper struct to hold column indices for a person's variables
type participantVars struct {
	work       map[int]int
	switches   map[int]int
	restBlocks map[int]int
}

func solveSchedule(data *InputData, timeLimit int) ([]ScheduleEntry, error) {

	// --- Initialize Problem ---
	lp := glpk.New()
	defer lp.Delete()
	lp.SetProbName("RaceScheduling")
	lp.SetObjDir(glpk.ObjDir(glpk.MIN))

	// --- Calculate Race Parameters ---
	if data.FuelUsePerLap <= 0 {
		return nil, fmt.Errorf("fuelUsePerLap must be > 0")
	}
	stintLaps := int(math.Floor(data.FuelTankSize / data.FuelUsePerLap))
	stintWithPitSeconds := (float64(stintLaps) * data.AvgLapTimeInSeconds) + data.PitTimeInSeconds
	raceDurationSeconds := float64(data.DurationHours * 3600)
	totalStints := int(math.Ceil(raceDurationSeconds / stintWithPitSeconds))

	if totalStints == 0 {
		return nil, fmt.Errorf("race calculations resulted in 0 stints")
	}
	log.Printf("--- Building 'integrated' schedule with %d stints ---", totalStints)

	raceStartUTC, err := time.Parse(time.RFC3339Nano, data.RaceStartUTC)
	if err != nil {
		return nil, fmt.Errorf("invalid raceStartUTC format: %w", err)
	}

	var driverPool, spotterPool []TeamMember
	for _, m := range data.TeamMembers {
		if m.IsDriver {
			driverPool = append(driverPool, m)
		}
		if m.IsSpotter {
			spotterPool = append(spotterPool, m)
		}
	}
	if len(driverPool) == 0 {
		return nil, fmt.Errorf("no drivers found in teamMembers")
	}

	// --- Create Solver Variables (Columns) ---
	// We need maps to translate our (name, stint) variables to GLPK's integer column indices.
	driveVars := make(map[string]*participantVars)
	spotVars := make(map[string]*participantVars)

	// Create all columns at once.
	numCols := 0
	// This function helps us create a variable and get its index
	createVar := func(name string, isBinary bool) int {
		numCols++
		lp.AddCols(1)
		lp.SetColName(numCols, name)
		if isBinary {
			lp.SetColKind(numCols, glpk.VarType(glpk.BV))
		} else {
			lp.SetColKind(numCols, glpk.VarType(glpk.IV))
			lp.SetColBnds(numCols, glpk.BndsType(glpk.LO), 0.0, 0.0)
		}
		return numCols
	}

	log.Println("Defining variables...")
	for _, p := range data.TeamMembers {
		if p.IsDriver {
			driveVars[p.Name] = &participantVars{work: make(map[int]int), switches: make(map[int]int)}
			for s := 0; s < totalStints; s++ {
				driveVars[p.Name].work[s] = createVar(fmt.Sprintf("Drive_%s_%d", p.Name, s), true)
				if s > 0 {
					driveVars[p.Name].switches[s] = createVar(fmt.Sprintf("DriveSwitch_%s_%d", p.Name, s), true)
				}
			}
		}
		if p.IsSpotter {
			spotVars[p.Name] = &participantVars{work: make(map[int]int), switches: make(map[int]int)}
			for s := 0; s < totalStints; s++ {
				spotVars[p.Name].work[s] = createVar(fmt.Sprintf("Spot_%s_%d", p.Name, s), true)
				if s > 0 {
					spotVars[p.Name].switches[s] = createVar(fmt.Sprintf("SpotSwitch_%s_%d", p.Name, s), true)
				}
			}
		}
	}

	// Objective function variables
	maxDriveStints := createVar("MaxDriveStints", false)
	minDriveStints := createVar("MinDriveStints", false)
	maxSpotStints := createVar("MaxSpotStints", false)
	minSpotStints := createVar("MinSpotStints", false)
	lp.SetObjCoef(maxDriveStints, 1000)
	lp.SetObjCoef(minDriveStints, -1000)
	lp.SetObjCoef(maxSpotStints, 1000)
	lp.SetObjCoef(minSpotStints, -1000)

	// --- Define Constraints (Rows) ---
	log.Println("Defining constraints...")

	// Helper to create a constraint row
	numRows := 0
	// This function signature is now correct for this library
	createConstraint := func(name string, boundsType glpk.BndsType, lower, upper float64) int {
		numRows++
		lp.AddRows(1)
		lp.SetRowName(numRows, name)
		lp.SetRowBnds(numRows, boundsType, lower, upper)
		return numRows
	}

	// 1. Availability Constraints
	for _, p := range data.TeamMembers {
		for s := 0; s < totalStints; s++ {
			stintStartTime := raceStartUTC.Add(time.Duration(float64(s)*stintWithPitSeconds) * time.Second)
			stintEndCheckTime := stintStartTime.Add(time.Duration(stintWithPitSeconds-1) * time.Second)
			startKey := stintStartTime.Truncate(time.Hour).Format(time.RFC3339Nano)
			endKey := stintEndCheckTime.Truncate(time.Hour).Format(time.RFC3339Nano)

			isAvailable := true
			if pAvail, ok := data.Availability[p.Name]; ok {
				if pAvail[startKey] == "Unavailable" || pAvail[endKey] == "Unavailable" {
					isAvailable = false
				}
				if pAvail[startKey] == "Preferred" {
					if p.IsDriver {
						lp.SetObjCoef(driveVars[p.Name].work[s], -1)
					}
					if p.IsSpotter {
						lp.SetObjCoef(spotVars[p.Name].work[s], -1)
					}
				}
			}

			if !isAvailable {
				// To enforce WorkVar = 0, we create a constraint: WorkVar <= 0
				if p.IsDriver {
					rowIdx := createConstraint(fmt.Sprintf("Unavailable_Drive_%s_%d", p.Name, s), glpk.BndsType(glpk.UP), 0, 0)
					lp.SetMatRow(rowIdx, []int32{int32(driveVars[p.Name].work[s])}, []float64{1.0})
				}
				if p.IsSpotter {
					rowIdx := createConstraint(fmt.Sprintf("Unavailable_Spot_%s_%d", p.Name, s), glpk.BndsType(glpk.UP), 0, 0)
					lp.SetMatRow(rowIdx, []int32{int32(spotVars[p.Name].work[s])}, []float64{1.0})
				}
			}
		}
	}

	// 2. Stint-level constraints
	for s := 0; s < totalStints; s++ {
		// One Driver Per Stint: sum(driveVars) == 1
		rowIdx := createConstraint(fmt.Sprintf("OneDriver_Stint_%d", s), glpk.BndsType(glpk.FX), 1.0, 1.0)
		indices := make([]int32, len(driverPool))
		coeffs := make([]float64, len(driverPool))
		for i, p := range driverPool {
			indices[i] = int32(driveVars[p.Name].work[s])
			coeffs[i] = 1.0
		}
		lp.SetMatRow(rowIdx, indices, coeffs)

		// At Most One Spotter Per Stint: sum(spotVars) <= 1
		rowIdx = createConstraint(fmt.Sprintf("AtMostOneSpotter_Stint_%d", s), glpk.BndsType(glpk.UP), 0.0, 1.0)
		indices = make([]int32, len(spotterPool))
		coeffs = make([]float64, len(spotterPool))
		for i, p := range spotterPool {
			indices[i] = int32(spotVars[p.Name].work[s])
			coeffs[i] = 1.0
		}
		lp.SetMatRow(rowIdx, indices, coeffs)

		// No Drive and Spot in Same Stint: Drive(p,s) + Spot(p,s) <= 1
		for _, p := range data.TeamMembers {
			if p.IsDriver && p.IsSpotter {
				rowIdx := createConstraint(fmt.Sprintf("NoDriveAndSpot_%s_%d", p.Name, s), glpk.BndsType(glpk.UP), 0, 1.0)
				lp.SetMatRow(rowIdx,
					[]int32{int32(driveVars[p.Name].work[s]), int32(spotVars[p.Name].work[s])},
					[]float64{1.0, 1.0},
				)
			}
		}
	}

	// 3. Participant-level constraints
	allParticipantConstraints := func(p TeamMember, vars *participantVars, minMaxVars [2]int, prefix string) {
		totalStintsExprIndices := make([]int32, totalStints)
		totalStintsExprCoeffs := make([]float64, totalStints)

		for s := 0; s < totalStints; s++ {
			totalStintsExprIndices[s] = int32(vars.work[s])
			totalStintsExprCoeffs[s] = 1.0

			// Max Consecutive: sum(work[s]..work[s+max]) <= max
			if p.PreferredStints > 0 && s <= totalStints-(p.PreferredStints+1) {
				rowIdx := createConstraint(fmt.Sprintf("MaxConsecutive_%s_%s_%d", prefix, p.Name, s), glpk.BndsType(glpk.UP), 0, float64(p.PreferredStints))
				indices := make([]int32, p.PreferredStints+1)
				coeffs := make([]float64, p.PreferredStints+1)
				for i := 0; i <= p.PreferredStints; i++ {
					indices[i] = int32(vars.work[s+i])
					coeffs[i] = 1.0
				}
				lp.SetMatRow(rowIdx, indices, coeffs)
			}

			// Switch Definition: switch[s] - work[s] + work[s-1] >= 0
			if s > 0 {
				lp.SetObjCoef(vars.switches[s], 100) // Add to objective
				rowIdx := createConstraint(fmt.Sprintf("DefineSwitch_%s_%s_%d", prefix, p.Name, s), glpk.BndsType(glpk.LO), 0, 0)
				lp.SetMatRow(rowIdx,
					[]int32{int32(vars.switches[s]), int32(vars.work[s]), int32(vars.work[s-1])},
					[]float64{1.0, -1.0, 1.0},
				)
			}
		}

		// Min/Max Definition for Objective
		// Max >= Total  => Max - Total >= 0
		rowIdxMax := createConstraint(fmt.Sprintf("DefineMax_%s_%s", prefix, p.Name), glpk.BndsType(glpk.LO), 0, 0)
		lp.SetMatRow(rowIdxMax, append([]int32{int32(minMaxVars[0])}, totalStintsExprIndices...), append([]float64{1.0}, negative(totalStintsExprCoeffs)...))

		// Min <= Total => Min - Total <= 0
		rowIdxMin := createConstraint(fmt.Sprintf("DefineMin_%s_%s", prefix, p.Name), glpk.BndsType(glpk.UP), 0, 0)
		lp.SetMatRow(rowIdxMin, append([]int32{int32(minMaxVars[1])}, totalStintsExprIndices...), append([]float64{1.0}, negative(totalStintsExprCoeffs)...))
	}

	for _, p := range driverPool {
		allParticipantConstraints(p, driveVars[p.Name], [2]int{maxDriveStints, minDriveStints}, "Drive")
	}
	for _, p := range spotterPool {
		allParticipantConstraints(p, spotVars[p.Name], [2]int{maxSpotStints, minSpotStints}, "Spot")
	}

	// 4. Driver Fair Share Rule: sum(driveStints) >= minStints
	if len(driverPool) > 0 && stintLaps > 0 {
		totalLaps := totalStints * stintLaps
		equalShareLaps := math.Floor(float64(totalLaps) / float64(len(driverPool)))
		minLaps := equalShareLaps / 4.0
		minStints := math.Ceil(minLaps / float64(stintLaps))
		log.Printf("Applying Fair Share Rule: Min %.0f stints per driver.", minStints)

		for _, p := range driverPool {
			rowIdx := createConstraint(fmt.Sprintf("FairShare_Drive_%s", p.Name), glpk.BndsType(glpk.LO), minStints, 0)
			indices := make([]int32, totalStints)
			coeffs := make([]float64, totalStints)
			for s := 0; s < totalStints; s++ {
				indices[s] = int32(driveVars[p.Name].work[s])
				coeffs[s] = 1.0
			}
			lp.SetMatRow(rowIdx, indices, coeffs)
		}
	}

	// 5. First Stint Driver
	if data.FirstStintDriver != "" {
		if _, ok := driveVars[data.FirstStintDriver]; ok {
			log.Printf("Adding constraint: First stint must be driven by %s", data.FirstStintDriver)
			rowIdx := createConstraint("FirstStintDriver", glpk.BndsType(glpk.FX), 1.0, 1.0)
			lp.SetMatRow(rowIdx, []int32{int32(driveVars[data.FirstStintDriver].work[0])}, []float64{1.0})
		} else {
			log.Printf("Warning: FirstStintDriver '%s' not found.", data.FirstStintDriver)
		}
	}

	// --- Solve! ---
	log.Printf("--- Solving... (Time limit: %ds) ---", timeLimit)

	iocp := glpk.NewIocp()
	iocp.SetPresolve(true)
	iocp.SetMsgLev(glpk.MsgLev(glpk.MSG_ERR))

	param := glpk.NewSmcp()
	param.SetMsgLev(glpk.MsgLev(glpk.MSG_ERR))

	// Note: GLPK's timeout is in milliseconds
	if timeLimit > 0 {
		// This library fork does not support setting a time limit.
		// iocp.TmLim = timeLimit * 1000  <- This field does not exist on this wrapper
		log.Println("Warning: 'lukpank/go-glpk' does not support setting a time limit. Solver will run until completion.")
	}

	err = lp.Simplex(param)
	if err != nil {
		return nil, fmt.Errorf("simplex solver failed: %w", err)
	}

	err = lp.Intopt(iocp)
	if err != nil {
		return nil, fmt.Errorf("integer solver failed: %w", err)
	}

	status := lp.MipStatus()
	if status != glpk.OPT && status != glpk.FEAS {
		return nil, fmt.Errorf("solver failed to find a solution. Status: %v", status)
	}

	if status == glpk.FEAS {
		log.Println("Warning: Solver stopped due to time limit before proving optimality. A valid schedule was found.")
	}

	// --- Process Results ---
	log.Println("Solution found. Processing results...")
	schedule := make([]ScheduleEntry, totalStints)
	for s := 0; s < totalStints; s++ {
		entry := ScheduleEntry{Stint: s + 1, Driver: "N/A", Spotter: "N/A"}
		for _, p := range driverPool {
			if lp.MipColVal(driveVars[p.Name].work[s]) > 0.5 {
				entry.Driver = p.Name
				break
			}
		}
		for _, p := range spotterPool {
			if lp.MipColVal(spotVars[p.Name].work[s]) > 0.5 {
				entry.Spotter = p.Name
				break
			}
		}
		schedule[s] = entry
	}

	return schedule, nil
}

// Helper to negate a slice of float64 for constraint building.
func negative(s []float64) []float64 {
	r := make([]float64, len(s))
	for i, v := range s {
		r[i] = -v
	}
	return r
}
