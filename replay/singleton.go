package replay

var singleton Service

//Singleton returns singleton  service
func Singleton() Service {
	if singleton != nil {
		return singleton
	}
	singleton := New()
	return singleton
}
