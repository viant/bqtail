package task

import "github.com/google/uuid"

//NextID generate next job ID
func NextID(prefix string) (string, error) {
	UUID, err := uuid.NewUUID()
	if err != nil {
		return "", err
	}
	return prefix + "_job_" + UUID.String(), nil
}
