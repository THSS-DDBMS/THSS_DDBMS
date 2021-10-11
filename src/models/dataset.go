package models

type Dataset struct {
	Schema TableSchema
	Rows []Row
}
