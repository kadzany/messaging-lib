package inbox

import (
	"context"
	"encoding/json"
	"time"

	"github.com/go-pg/pg"
	"github.com/google/uuid"
	"github.com/kadzany/messaging-lib/common"
)

type Inboxes struct {
	ID          string                 `pg:"id,type:uuid,default:uuid_generate_v4()"`
	Payload     map[string]interface{} `pg:"type:jsonb,notnull"`
	Topic       string                 `pg:"type:varchar(255),notnull"`
	IsAccepted  bool                   `pg:"type:bool,notnull,default:false"`
	Publisher   string                 `pg:"type:varchar(255),default:null"`
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
	return im.db.RunInTransaction(func(tx *pg.Tx) error {
		var payload map[string]interface{}
		if err := json.Unmarshal(message.Payload, &payload); err != nil {
			return err
		}
		inbox := &Inboxes{
			ID:         uuid.NewString(),
			Payload:    payload,
			Topic:      message.Topic,
			IsAccepted: false,
			CreatedAt:  time.Now(),
		}
		_, err := tx.Model(inbox).Insert()
		if err != nil {
			return err
		}

		if err := im.handler.Dispatch(ctx, message); err != nil {
			return err
		}

		_, err = tx.Model(inbox).
			Set("is_accepted = ?", true).
			Set("processed_at = ?", time.Now()).
			WherePK().
			Update()
		return err
	})
}
