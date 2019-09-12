package base

import "strings"

//DecodePathSeparator decode job ID
func DecodePathSeparator(jobID string) string {
	if count := strings.Count(jobID, PathElementSeparator); count > 0 {
		jobID = strings.Replace(jobID, PathElementSeparator, "/", count)
	}
	return jobID
}

//EncodePathSeparator encodes job ID
func EncodePathSeparator(jobID string) string {
	if count := strings.Count(jobID, "/"); count > 0 {
		jobID = strings.Replace(jobID, "/", PathElementSeparator, count)
	}
	return jobID
}
