package inbox

import (
	"context"
	"time"

	"github.com/go-pg/pg"
	"github.com/kadzany/messaging-lib/common"
)

type Inboxes struct {
	ID          string                 `pg:"id,type:uuid,default:uuid_generate_v4()"`
	Payload     map[string]interface{} `pg:"type:jsonb,notnull"`
	Topic       string                 `pg:"type:varchar(255),notnull"`
	Status      string                 `pg:"type:varchar(255),default:null"`
	CreatedAt   time.Time              `pg:"type:timestamptz,default:now()"`
	ProcessedAt time.Time              `pg:"type:timestamptz,default:null"`
	CreatedBy   string                 `pg:"type:varchar(255)"`
	UpdatedAt   time.Time              `pg:"type:timestamptz,default:now()"`
	UpdatedBy   string                 `pg:"type:varchar(255)"`
}

type MessageHandler interface {
	Dispatch(ctx context.Context, message common.Message) error
}

type InboxManager struct {
	db      *pg.DB
	handler MessageHandler
}

func NewInboxManager(db *pg.DB) *InboxManager {
	return &InboxManager{db: db}
}

func (im *InboxManager) ProcessMessage(ctx context.Context, message common.Message) error {
	if err := im.handler.Dispatch(ctx, message); err != nil {
		return err
	}

	return nil
}
