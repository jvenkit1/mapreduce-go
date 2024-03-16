package mapreduce

import (
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/rs/zerolog/log"
)

func (leader *Leader) splitTasks() {
	// Do we need this?
}

func (leader *Leader) isIntermediateFile(fileName string) bool {
	return strings.HasPrefix(fileName, "intermediate-")
}

// Invoked from the "reduce" phase
// Function merges all the intermediate output files into one single file
// Invocation: Invoked at the end of all reduce operations.
// Paper describes using a GFS for storing all the files. This implementation writes data into folder "intermediate"
func (leader *Leader) mergeTasks(opName string) {
	log.Info().Msgf("Merging all intermediate files into one output file")

	var intermediateDir string = "intermediate/" + opName
	var outputFileName string = "output-" + opName + ".txt"
	files, err := os.ReadDir(intermediateDir)
	if err != nil {
		log.Fatal().Err(err).Msgf("Directory read failed for operation %s", opName)
	}

	var waitGroup sync.WaitGroup
	var synchronizerMutex sync.Mutex // Mutex variable to synchronize writes to the file

	outputFile, err := os.OpenFile(outputFileName, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal().Err(err).Msgf("Unable to create/open file %s", outputFileName)
	}

	defer outputFile.Close()

	for _, intermediateFile := range files {
		intermediateFileName := intermediateFile.Name()
		if leader.isIntermediateFile(intermediateFileName) {
			waitGroup.Add(1)
			go func(fileName string) {
				defer waitGroup.Done()
				filePath := fmt.Sprintf("%s/%s", intermediateDir, intermediateFileName)

				content, err := os.ReadFile(filePath)
				if err != nil {
					log.Fatal().Err(err).Msgf("Unable to read file %s", intermediateFileName)
				}

				synchronizerMutex.Lock()
				defer synchronizerMutex.Unlock()

				if _, err = outputFile.Write(content); err != nil {
					log.Fatal().Err(err).Msgf("Unable to write to file %s", outputFileName)
				}
			}(intermediateFileName)
		}
	}
	waitGroup.Wait()

	log.Info().Msgf("Finished merging all intermediate files for operation %s", opName)
}
