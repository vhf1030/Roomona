import pymysql
from config.my_config import PYMYSQL_CONNECT


conn = pymysql.connect(
    user=PYMYSQL_CONNECT['user'],
    passwd=PYMYSQL_CONNECT['passwd'],
    host=PYMYSQL_CONNECT['host'],
    db=PYMYSQL_CONNECT['db'],
    charset='utf8mb4',
)
cursor = conn.cursor(pymysql.cursors.DictCursor)


def create_cafe_table():
    sql = '''CREATE TABLE `escape_cafe_jb` (
    `cafe_id` SMALLINT PRIMARY KEY,
    `cafe_name` VARCHAR(63) NOT NULL,
    `location_category` VARCHAR(15) NOT NULL,
    `location_detail` VARCHAR(255) NOT NULL,
    `star_rate` DECIMAL(3, 2) NOT NULL,
    `star_num` SMALLINT NOT NULL,
    `updated` TIMESTAMP NOT NULL DEFAULT NOW(),
    `deleted` BOOLEAN NOT NULL DEFAULT 0,
    INDEX (`location_category`)
    )ENGINE = MyISAM;
    '''
    # `home_url` VARCHAR(255),  # cafe detail 에서 수집 예정
    cursor.execute(sql)
    conn.commit()
    print(sql)
    return


def create_theme_table():
    sql = '''CREATE TABLE `escape_theme_jb` (
    `theme_id` SMALLINT PRIMARY KEY,
    `theme_name` VARCHAR(63) NOT NULL,
    `cafe_id` SMALLINT NOT NULL,
    `cafe_name` VARCHAR(63) NOT NULL,
    `star_rate` DECIMAL(3, 2) NOT NULL,
    `updated` TIMESTAMP NOT NULL DEFAULT NOW(),
    `deleted` BOOLEAN NOT NULL DEFAULT 0,
    INDEX (`cafe_id`)
    )ENGINE = MyISAM;
    '''
    # `home_url` VARCHAR(255),  # cafe detail 에서 수집 예정
    cursor.execute(sql)
    conn.commit()
    print(sql)
    return


# filtering 이후 진행 (테마평점 3점 이상)
def create_cafe_url_table():
    sql = '''CREATE TABLE `escape_cafe_url_jb` (
    `cafe_id` SMALLINT PRIMARY KEY,
    `home_url` VARCHAR(255)
    )ENGINE = MyISAM;
    '''
    cursor.execute(sql)
    conn.commit()
    print(sql)
    return


def create_theme_meta_table():
    sql = '''CREATE TABLE `escape_theme_meta_jb` (
    `theme_id` SMALLINT PRIMARY KEY,
    `limit_time` SMALLINT NOT NULL,
    `genre` VARCHAR(63) NOT NULL,
    `difficulty` TINYINT NOT NULL,
    `lock_type` VARCHAR(15) NOT NULL,
    `activity` VARCHAR(15) NOT NULL,
    `recom_user` VARCHAR(15) NOT NULL
    )ENGINE = MyISAM;
    '''
    cursor.execute(sql)
    conn.commit()
    print(sql)
    return


def create_theme_review_stat_table():
    sql = '''CREATE TABLE `escape_theme_review_stat_jb` (
    `theme_id` SMALLINT NOT NULL,
    `user_name` VARCHAR(63) NOT NULL,
    `review_date` DATE NOT NULL,
    `medal` TINYINT,
    `difficulty` TINYINT NOT NULL,
    `success` BOOLEAN NOT NULL,
    `star` DECIMAL(2, 1),
    `left_time` SMALLINT,
    `hint` SMALLINT,
    `updated` TIMESTAMP NOT NULL DEFAULT NOW(),
    `deleted` BOOLEAN NOT NULL DEFAULT 0,
    UNIQUE INDEX `theme_user_date_UNIQUE` (`theme_id`, `user_name`, `review_date`),
    INDEX (`user_name`)
    )ENGINE = MyISAM;
    '''
    cursor.execute(sql)
    conn.commit()
    print(sql)
    return


def create_theme_review_text_table():
    sql = '''CREATE TABLE `escape_theme_review_text_jb` (
    `theme_id` SMALLINT NOT NULL,
    `user_name` VARCHAR(63) NOT NULL,
    `review_date` DATE NOT NULL,
    `review_txt` TEXT,
    `check_date` DATE NOT NULL DEFAULT NOW(),
    `updated` TIME NOT NULL DEFAULT NOW(),
    `deleted` BOOLEAN NOT NULL DEFAULT 0,
    UNIQUE INDEX `theme_user_date_UNIQUE` (`theme_id`, `user_name`, `review_date`, `check_date`),
    INDEX (`user_name`)
    )ENGINE = MyISAM;
    '''
    cursor.execute(sql)
    conn.commit()
    print(sql)
    return


def create_brand_theme_table():
    sql = '''CREATE TABLE `escape_brand_theme` (
    `brand_num` SMALLINT NOT NULL,
    `brand_name` VARCHAR(63) NOT NULL,
    `branch_num` SMALLINT NOT NULL,
    `branch_name` VARCHAR(63) NOT NULL,
    `theme_num` SMALLINT NOT NULL,
    `theme_name` VARCHAR(63) NOT NULL,
    `request_key` VARCHAR(63),
    `theme_id` SMALLINT DEFAULT NULL,
    `updated` TIMESTAMP NOT NULL DEFAULT NOW(),
    `deleted` BOOLEAN NOT NULL DEFAULT 0,
    UNIQUE INDEX `theme_user_date_UNIQUE` (`brand_num`, `branch_num`, `theme_num`),
    INDEX (`theme_id`)
    )ENGINE = MyISAM;
    '''
    cursor.execute(sql)
    conn.commit()
    print(sql)
    return


def create_theme_reserve_table():
    sql = '''CREATE TABLE `escape_theme_reserve` (
    `theme_id` SMALLINT NOT NULL,
    `rsv_date` DATE NOT NULL,
    `rsv_time` TIME NOT NULL,
    `available` BOOLEAN NOT NULL,
    `chk_date` DATE NOT NULL,
    `updated` TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE INDEX `theme_user_date_UNIQUE` (`theme_id`, `rsv_date`, `rsv_time`, `chk_date`),
    INDEX (`theme_id`),
    INDEX (`rsv_date`)
    )ENGINE = MyISAM;
    '''
    cursor.execute(sql)
    conn.commit()
    print(sql)
    return

