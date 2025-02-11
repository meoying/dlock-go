package gorm

import (
	glock "github.com/meoying/dlock/internal/gorm"
	"gorm.io/gorm"
)

func NewClient(db *gorm.DB) *glock.Client {
	return glock.NewClient(db)
}
