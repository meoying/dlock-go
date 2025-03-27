package gorm

import (
	glock "github.com/meoying/dlock-go/internal/gorm"
	"gorm.io/gorm"
)

// NewCASFirstClient 加锁的模式是 CAS First
func NewCASFirstClient(db *gorm.DB) *glock.Client {
	return glock.NewCASFirstClient(db)
}

// NewInsertFirstClient 加锁的模式是 Insert First
func NewInsertFirstClient(db *gorm.DB) *glock.Client {
	return glock.NewInsertFirstClient(db)
}
