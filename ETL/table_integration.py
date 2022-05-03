from ETL.db_handle import execute_sql


def integ_jb_brand():
    # brand theme table에 플랫폼 theme id를 부여
    sql = '''SELECT c.location_category, c.cafe_name, t.theme_id, t.theme_name
    FROM roomona.escape_theme_jb as t
    LEFT JOIN roomona.escape_cafe_jb as c
        ON t.cafe_id = c.cafe_id
    WHERE c.cafe_name is not null'''
    theme_jb = execute_sql(sql)

    sql = '''SELECT brand_num, brand_name, branch_num, branch_name, theme_num, theme_name
    FROM roomona.escape_brand_theme;'''
    theme_brand = execute_sql(sql)

    for i, tb in enumerate(theme_brand.iloc):
        # print(tb)
        print(i, tb['theme_name'])
        t_len, check_1 = len(tb['theme_name']), []
        while t_len > 0 and len(check_1) < 1:
            check_1 = theme_jb[(theme_jb['theme_name'].str[:t_len] == tb['theme_name'][:t_len])]
            if len(check_1) != 1:
                check_1 = theme_jb[(theme_jb['theme_name'].str[-t_len:] == tb['theme_name'][-t_len:])]
            t_len -= 1
        branch_name_tmp = tb['branch_name'].split(' ')[0]
        check_2 = check_1[check_1['cafe_name'].str.contains(tb['brand_name']) &
                          check_1['cafe_name'].str.contains(branch_name_tmp)]
        # branch_name_tmp = tb['branch_name'].replace('Ⅱ', 'II')
        # check_2 = check_1[check_1['cafe_name'].str.contains(tb['brand_name']) & check_1['cafe_name'].str.contains(branch_name_tmp)]
        # if len(check_2) < 1:
        #     branch_name_tmp = branch_name_tmp.replace(' ', '')
        #     check_2 = check_1[check_1['cafe_name'].str.contains(tb['brand_name']) & check_1['cafe_name'].str.contains(branch_name_tmp)]
        if len(check_2) == 1:
            sql = '''UPDATE roomona.escape_brand_theme SET theme_id = %s, updated = default
            WHERE brand_num = %s AND branch_num = %s AND theme_num = %s;''' % (
                check_2['theme_id'].values[0], tb['brand_num'], tb['branch_num'], tb['theme_num']
            )
            print(check_2)
            execute_sql(sql)
        if len(check_2) != 1:
            print('fail', len(check_2), len(check_1), tb, check_1, '\n')
    return


def jb_stats_product_db():
    execute_sql('DROP TABLE IF EXISTS roomona.escape_jb_stats;')
    sql = '''
    CREATE TABLE roomona.escape_jb_stats AS 
    SELECT
        c.cafe_id,
        c.cafe_name,
        c.location_category,
        c.location_detail,
        cm.home_url,
        t.theme_id,
        t.theme_name,
        tm.genre,
        tm.activity,
        tm.recom_user,
        COUNT(trs.star) review_n,
        AVG(trs.star) star_stat,
        AVG(trs.medal) medal_stat,
        AVG(trs.difficulty) diff_stat,
        AVG(trs.success) success_rate,
        AVG(trs.left_time) left_stat,
        tm.limit_time,
        AVG(trs.hint) hint_stst,
        trs.updated trs_updated
    FROM
        roomona.escape_theme_jb AS t
            LEFT JOIN
        roomona.escape_cafe_jb AS c ON t.cafe_id = c.cafe_id
            LEFT JOIN
        roomona.escape_cafe_meta_jb AS cm ON c.cafe_id = cm.cafe_id
            LEFT JOIN
        roomona.escape_theme_meta_jb AS tm ON t.theme_id = tm.theme_id
            RIGHT JOIN
        roomona.escape_theme_review_stat_jb AS trs ON t.theme_id = trs.theme_id
    WHERE
        location_category IS NOT NULL
    GROUP BY theme_id
    ORDER BY star_stat DESC;
    '''
    execute_sql(sql)
    return







