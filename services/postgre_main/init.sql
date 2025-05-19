CREATE TABLE pitching_data (
    id SERIAL PRIMARY KEY,
    start_speed FLOAT NOT NULL,
    spin_rate FLOAT NOT NULL,
    extension FLOAT NOT NULL,
    az FLOAT NOT NULL,
    ax FLOAT NOT NULL,
    x0 FLOAT NOT NULL,
    z0 FLOAT NOT NULL,
    speed_diff FLOAT NOT NULL,
    az_diff FLOAT NOT NULL,
    ax_diff FLOAT NOT NULL,
    delta_run_value FLOAT NOT NULL
);

-- 권한 설정
ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT ALL ON TABLES TO juyeon1;

ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT ALL ON SEQUENCES TO juyeon1;

GRANT CREATE ON SCHEMA public TO juyeon1;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO juyeon1;