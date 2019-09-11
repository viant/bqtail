package base

import "strings"

//DecodeID decode job ID
func DecodeID(jobID string) string {
	if count := strings.Count(jobID, PathElementSeparator); count > 0 {
		jobID = strings.Replace(jobID, PathElementSeparator, "/", count)
	}
	return jobID
}


//EncodeID encodes job ID
func EncodeID(jobID string) string {
	if count := strings.Count(jobID, "/"); count > 0 {
		jobID = strings.Replace(jobID, "/", PathElementSeparator, count)
	}
	return jobID
}
