create_schema = """
    DROP SCHEMA IF EXISTS pubg CASCADE;
    CREATE SCHEMA pubg;
"""
create_match_table = """
    DROP TABLE IF EXISTS pubg.match;
    CREATE TABLE pubg.match (
        match_id VARCHAR(256) NOT NULL PRIMARY KEY,
        map VARCHAR(256),
        match_mode VARCHAR(256),
        game_size INTEGER
    );
"""
create_time_table = """
    DROP TABLE IF EXISTS pubg."time";
    CREATE TABLE pubg."time" (
        "timestamp" TIMESTAMP NOT NULL PRIMARY KEY,
        "year" INTEGER,
        "month" INTEGER,
        "day" INTEGER,
        "week" INTEGER,
        "weekday" INTEGER,
        "hour" INTEGER
    );
"""

create_player_table = """
    DROP TABLE IF EXISTS pubg.player;
    CREATE TABLE pubg.player (
        player_id VARCHAR(256) NOT NULL PRIMARY KEY,
        match_id VARCHAR(256) NOT NULL,
        player_name VARCHAR(256) NOT NULL,
        party_size INTEGER,
        player_assists INTEGER,
        player_dbno INTEGER,
        player_dist_ride NUMERIC(18, 2),
        player_dist_walk NUMERIC(18, 2),
        player_dmg INTEGER,
        player_kills INTEGER,
        player_survive_time NUMERIC(8, 0),
        team_id INTEGER,
        team_placement INTEGER
    );
"""

create_weapon_table = """
    DROP TABLE IF EXISTS pubg.weapon;
    CREATE TABLE pubg.weapon (
        weapon VARCHAR(256) NOT NULL PRIMARY KEY,
        weapon_type VARCHAR(256),
        bullet_type NUMERIC(6,2),
        damage INTEGER,
        megazine_capacity INTEGER,
        range INTEGER,
        bullet_speed INTEGER,
        rate_of_fire NUMERIC(10,5)
    );
"""

create_kill_log_table = """
    DROP TABLE IF EXISTS pubg.kill_log;
    CREATE TABLE pubg.kill_log (
        kill_log_id INTEGER NOT NULL PRIMARY KEY,
        killer_id VARCHAR(256) NOT NULL,
        victim_id VARCHAR(256) NOT NULL,
        match_id VARCHAR(256) NOT NULL,
        "timestamp" TIMESTAMP NOT NULL,
        weapon VARCHAR(256) NOT NULL,
        killer_position_x NUMERIC(10,2),
        killer_position_y NUMERIC(10,2),
        victim_position_x NUMERIC(10,2),
        victim_position_y NUMERIC(10,2)
    );
"""

data_check_list = [
    {'dq_check': "SELECT COUNT(*) FROM {0} WHERE {1} is not null",
    'expected_result': "SELECT count(*) FROM {}",
    
    'success': "At least 1 record found and No NUll vales from {}",
    'fail': "No records found or Null value found from {}"}
    
]