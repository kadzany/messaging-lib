package outbox

import (
	"time"
)

type Outboxes struct {
	ID          string                 `pg:"id,type:uuid,default:uuid_generate_v4(),pk"`
	Payload     map[string]interface{} `pg:"type:jsonb,notnull"`
	Key         string                 `pg:"type:varchar(255)"`
	IsDeliver   bool                   `pg:"type:bool,notnull,default:false"`
	Topic       string                 `pg:"type:varchar(255),notnull"`
	CreatedAt   time.Time              `pg:"type:timestamptz,default:now()"`
	ProcessedAt time.Time              `pg:"type:timestamptz,default:null"`
	CreatedBy   string                 `pg:"type:varchar(255)"`
	UpdatedAt   time.Time              `pg:"type:timestamptz,default:now()"`
	UpdatedBy   string                 `pg:"type:varchar(255)"`
}
