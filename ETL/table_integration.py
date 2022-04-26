from ETL.db_handle import execute_sql, upsert_brand_theme


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












