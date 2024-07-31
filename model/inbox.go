package model

import (
	"time"

	"github.com/google/uuid"
)

type Inbox struct {
	ID             uuid.UUID `pg:"type:uuid,default:uuid_generate_v4()"`
	MessagePayload string    `pg:"type:text,notnull"`
	Status         string    `pg:"type:varchar(10),notnull"`
	TopicName      string    `pg:"type:varchar(255),notnull"`
	Publisher      string    `pg:"type:varchar(255)"`
	CreatedAt      time.Time `pg:"type:timestamptz,default:now()"`
	CreatedBy      string    `pg:"type:varchar(255)"`
	UpdatedAt      time.Time `pg:"type:timestamptz,default:now()"`
	UpdatedBy      string    `pg:"type:varchar(255)"`
}
