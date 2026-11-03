-- 投影表（MySQL 5.7+/8.x、MariaDB 10.x，InnoDB）。所有金额/数量/价格均为 BIGINT 定点整数。
CREATE TABLE IF NOT EXISTS projection_checkpoint (
  id        TINYINT NOT NULL PRIMARY KEY,
  last_seq  BIGINT  NOT NULL
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS account_balance (
  account_id BIGINT NOT NULL,
  asset      INT    NOT NULL,
  available  BIGINT NOT NULL DEFAULT 0,
  frozen     BIGINT NOT NULL DEFAULT 0,
  PRIMARY KEY (account_id, asset)
) ENGINE=InnoDB;

-- status: 0=open 1=filled 2=cancelled
CREATE TABLE IF NOT EXISTS orders (
  order_id        BIGINT  NOT NULL PRIMARY KEY,
  account_id      BIGINT  NOT NULL,
  side            TINYINT NOT NULL,
  price           BIGINT  NOT NULL,
  qty             BIGINT  NOT NULL,
  remaining       BIGINT  NOT NULL,
  status          TINYINT NOT NULL,
  client_order_id BIGINT  NOT NULL,
  created_ts      BIGINT  NOT NULL,
  updated_seq     BIGINT  NOT NULL,
  KEY idx_orders_account (account_id, status)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS trades (
  seq            BIGINT  NOT NULL PRIMARY KEY,
  taker_order_id BIGINT  NOT NULL,
  maker_order_id BIGINT  NOT NULL,
  taker_account  BIGINT  NOT NULL,
  maker_account  BIGINT  NOT NULL,
  taker_side     TINYINT NOT NULL,
  price          BIGINT  NOT NULL,
  qty            BIGINT  NOT NULL,
  ts             BIGINT  NOT NULL,
  KEY idx_trades_taker (taker_account, seq),
  KEY idx_trades_maker (maker_account, seq)
) ENGINE=InnoDB;
