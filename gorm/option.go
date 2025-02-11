package gorm

import (
	"github.com/ecodeclub/ekit/bean/option"
	glock "github.com/meoying/dlock/internal/gorm"
)

func WithTableName(tableName string) option.Option[glock.Lock] {
	return glock.WithTableName(tableName)
}

func WithMode(mode string) option.Option[glock.Lock] {
	return glock.WithMode(mode)
}
