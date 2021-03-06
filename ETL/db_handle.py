import pymysql
import pandas as pd
from config.my_config import PYMYSQL_CONNECT


conn = pymysql.connect(
    user=PYMYSQL_CONNECT['user'],
    passwd=PYMYSQL_CONNECT['passwd'],
    host=PYMYSQL_CONNECT['host'],
    db=PYMYSQL_CONNECT['db'],
    charset='utf8mb4',
)
cursor = conn.cursor(pymysql.cursors.DictCursor)


def execute_sql(sql):
    cursor.execute(sql)
    result = cursor.fetchall()
    return pd.DataFrame(result)
# sql = '''SELECT c.location_category, c.cafe_name, t.theme_id, t.theme_name, t.star_rate
# FROM roomona.escape_theme_jb as t
# LEFT JOIN roomona.escape_cafe_jb as c
# ON t.cafe_id = c.cafe_id
# WHERE c.cafe_name is not null
# and t.star_rate >= 4
# ORDER BY t.star_rate DESC;'''
# test = execute_sql(sql)

# sql = '''SELECT * FROM roomona.escape_cafe_jb'''
# test = execute_sql(sql)


def execute_sql_list(sql_list):
    for sql in sql_list:
        print(sql)
        cursor.execute(sql)
        cursor.fetchall()
    return


def upsert_cafe_table(ci_list):
    columns = ['cafe_id', 'cafe_name', 'location_category', 'location_detail', 'star_rate', 'star_num']
    val = [tuple(ci[c] for c in columns) for ci in ci_list]
    sql = ("INSERT INTO `escape_cafe_jb` (" + ', '.join(columns) +
           ") VALUES (%s, %s, %s, %s, %s, %s)" +
           " ON DUPLICATE KEY UPDATE " +
           ', '.join([c + ' = VALUES(' + c + ')' for c in columns]) +
           ";")
    cursor.executemany(sql, val)
    conn.commit()
    return
# for loc in ['강남', '건대', '신림', '신촌', '홍대', '대학로', '강북', '서울(기타)']:
#     pcl = post_cafe_list_jb(loc)
#     upsert_cafe_table(pcl)


def upsert_theme_table(ci_list):
    columns = ['theme_id', 'theme_name', 'cafe_id', 'cafe_name', 'star_rate']
    val = [tuple(ci[c] for c in columns) for ci in ci_list]
    sql = ("INSERT INTO `escape_theme_jb` (" + ', '.join(columns) +
           ") VALUES (%s, %s, %s, %s, %s)" +
           " ON DUPLICATE KEY UPDATE " +
           ', '.join([c + ' = VALUES(' + c + ')' for c in columns]) +
           ";")
    cursor.executemany(sql, val)
    conn.commit()
    return
# upsert_theme_table(ptl)


# # filtering 이후 진행 (테마평점 3점 이상)
# sql = '''SELECT c.cafe_id, c.cafe_name, t.theme_id, t.theme_name, t.star_rate
# FROM roomona.escape_theme_jb as t LEFT JOIN roomona.escape_cafe_jb as c ON t.cafe_id = c.cafe_id
# WHERE c.cafe_name is not null and t.star_rate >= 3;'''
# filted = execute_sql(sql)


def upsert_cafe_url(cafe_url_list):
    columns = ['cafe_id', 'home_url']
    val = [tuple(cm[c] for c in columns) for cm in cafe_url_list]
    sql = ("INSERT INTO `escape_cafe_url_jb` (" + ', '.join(columns) +
           ") VALUES (" + ', '.join(['%s']*len(columns)) +
           ") ON DUPLICATE KEY UPDATE " +
           ', '.join([c + ' = VALUES(' + c + ')' for c in columns]) +
           ";")
    cursor.executemany(sql, val)
    conn.commit()
    return
# cafe_url_list = [get_cafe_url_jb(cid) for cid in set(filted['cafe_id'].values)]
# upsert_cafe_url(cafe_url_list)
# # cafe_meta_list = [get_cafe_detail_jb(cid)['metadata'] for cid in set(filted['cafe_id'].values)]
# # upsert_cafe_meta(cafe_meta_list)


def upsert_theme_meta(theme_meta_list):
    columns = ['theme_id', 'limit_time', 'genre', 'difficulty', 'lock_type', 'activity', 'recom_user']
    val = [tuple(cm[c] for c in columns) for cm in theme_meta_list]
    sql = ("INSERT INTO `escape_theme_meta_jb` (" + ', '.join(columns) +
           ") VALUES (" + ', '.join(['%s']*len(columns)) +
           ") ON DUPLICATE KEY UPDATE " +
           ', '.join([c + ' = VALUES(' + c + ')' for c in columns]) +
           ";")
    cursor.executemany(sql, val)
    conn.commit()
    return


def upsert_theme_review_stat(theme_review_stat_list):
    columns = ['theme_id', 'user_name', 'review_date', 'medal', 'difficulty', 'success', 'star', 'left_time', 'hint']
    val = [tuple(cm[c] for c in columns) for cm in theme_review_stat_list]
    sql = ("INSERT INTO `escape_theme_review_stat_jb` (" + ', '.join(columns) +
           ") VALUES (" + ', '.join(['%s']*len(columns)) +
           ") ON DUPLICATE KEY UPDATE " +
           ', '.join([c + ' = VALUES(' + c + ')' for c in columns]) +
           ";")
    cursor.executemany(sql, val)
    conn.commit()
    return


def upsert_theme_review_text(theme_review_text_list):
    columns = ['theme_id', 'user_name', 'review_date', 'review_txt']
    val = [tuple(cm[c] for c in columns) for cm in theme_review_text_list]
    sql = ("INSERT INTO `escape_theme_review_text_jb` (" + ', '.join(columns) +
           ") VALUES (" + ', '.join(['%s']*len(columns)) +
           ") ON DUPLICATE KEY UPDATE " +
           ', '.join([c + ' = VALUES(' + c + ')' for c in columns]) +
           ";")
    cursor.executemany(sql, val)
    conn.commit()
    return


# # for tid in filted['theme_id'].values:
# # # for tid in [3528, 3531, 3540, 3030, 2008, 3547, 3548, 3549, 3551, 2019, 2020, 3569, 3572, 3580, 3583]:  # 3526 error
# # for tid in ebt.theme_id.values:
# # sql = '''SELECT * FROM roomona.escape_brand_theme
# # WHERE theme_id is not null'''
# # ebt = execute_sql(sql)

# tid_tmp = list(ebt.theme_id.values)
# while tid_tmp:
#     tid = tid_tmp.pop(0)
#     print(len(tid_tmp), filted.loc[filted['theme_id'] == tid])
#     gtd = get_theme_detail_jb(tid)
#     upsert_theme_meta([gtd['metadata']])
#     upsert_theme_review_stat(gtd['review_stats'])
#     upsert_theme_review_text(gtd['review_texts'])


def upsert_theme_detail(gtd):
    # gtd = get_theme_detail_jb(tid)
    upsert_theme_meta([gtd['metadata']])
    upsert_theme_review_stat(gtd['review_stats'])
    upsert_theme_review_text(gtd['review_texts'])


# TODO: db table column 정보 활용하기 - column name 하드코딩 제거 및 updated 갱신


def upsert_brand_theme(brand_theme_list):
    columns = ['brand_num', 'brand_name', 'branch_num', 'branch_name', 'theme_num', 'theme_name', 'request_key']
    val = [tuple(cm[c] for c in columns) for cm in brand_theme_list]
    sql = ("INSERT INTO `escape_brand_theme` (" + ', '.join(columns) +
           ") VALUES (" + ', '.join(['%s']*len(columns)) +
           ") ON DUPLICATE KEY UPDATE " +
           ', '.join([c + ' = VALUES(' + c + ')' for c in columns]) +
           ";")
    cursor.executemany(sql, val)
    conn.commit()
    return
# upsert_brand_theme(find_theme_keyescape())
# upsert_brand_theme(find_theme_xphobia())
# upsert_brand_theme(find_theme_secretgarden())
# upsert_brand_theme(find_theme_zeroworld())
# upsert_brand_theme(find_theme_nextedition())


def update_brand_theme_id(theme_id, match_score, brand_n, branch_n, theme_n):
    sql = '''UPDATE roomona.escape_brand_theme SET theme_id = %s, match_score = %s, updated = default
                WHERE brand_num = %s AND branch_num = %s AND theme_num = %s;''' % (
        theme_id, match_score, brand_n, branch_n, theme_n
    )
    execute_sql(sql)
    return


def upsert_theme_reserve(theme_reserve_list):
    columns = ['theme_id', 'rsv_date', 'rsv_time', 'available', 'chk_date']
    val = [tuple(cm[c] for c in columns) for cm in theme_reserve_list]
    sql = ("INSERT INTO `escape_theme_reserve` (" + ', '.join(columns) +
           ") VALUES (" + ', '.join(['%s']*len(columns)) +
           ") ON DUPLICATE KEY UPDATE " +
           ', '.join([c + ' = VALUES(' + c + ')' for c in columns]) +
           ", updated = default;")  # update 갱신
    cursor.executemany(sql, val)
    conn.commit()
    return

# upsert_theme_reserve(check_reservation(3498, '2022-05-04'))
# upsert_theme_reserve(check_reservation(3498, '2022-05-03'))
# upsert_theme_reserve(check_reservation(3498, '2022-05-02'))

