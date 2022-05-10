from config.my_config import SQL_QUERY
from ETL.jb_scrap import post_cafe_list_jb, post_theme_list_jb, get_cafe_url_jb, get_theme_detail_jb
from ETL.brand_scrap import find_brand_theme
from ETL.db_handle import execute_sql, execute_sql_list, upsert_cafe_table, upsert_theme_table, upsert_cafe_url,\
    upsert_theme_detail, upsert_brand_theme, update_brand_theme_id
import time


def run_jb_scrap():
    # 일주일 한 번 업데이트
    # escape_cafe_jb
    for loc in ['강남', '건대', '신림', '신촌', '홍대', '대학로', '강북', '서울(기타)']:
        pcl = post_cafe_list_jb(loc)
        upsert_cafe_table(pcl)
    # escape_theme_jb
    ptl = post_theme_list_jb('전국')
    upsert_theme_table(ptl)

    filted = execute_sql(SQL_QUERY['select_jb_cafe_theme'])  # 테마 평점 3점 이상

    # escape_cafe_url_jb
    cafe_url_list = [get_cafe_url_jb(cid) for cid in set(filted['cafe_id'].values)]
    upsert_cafe_url(cafe_url_list)

    # escape_theme_meta_jb, escape_theme_review_stat_jb, escape_theme_review_text_jb
    theme_list = list(filted['theme_id'].values)
    print('start theme detail:', len(theme_list))
    while theme_list:
        print(len(theme_list), tid)
        try:
            tid = theme_list.pop()
            gtd = get_theme_detail_jb(tid)
            upsert_theme_detail(gtd)
        except:
            print('sleep')
            theme_list.append(tid)
            time.sleep(10)

    # escape_jb_stats - 통합
    execute_sql_list(SQL_QUERY['integ_jb_stats_query_list'])
    return



def run_brand_theme():
    # 일주일 한 번 업데이트
    # TODO: 삭제된 테마 처리 필요
    for brand in ['keyescape', 'xphobia', 'secretgarden', 'zeroworld', 'nextedition']:
        bt = find_brand_theme(brand)
        # escape_brand_theme
        upsert_brand_theme(bt)

    # jb data theme_id 연동(중복 처리 필요)
    tb_jb = execute_sql('select * from roomona.escape_jb_stats')
    tb_br = execute_sql('select * from roomona.escape_brand_theme')
    tb_jb_theme = tb_jb['theme_name'].str.replace(' ', '')

    def check_match_score(theme_name):
        score_dict = {}
        for i in range(1, len(theme_name) + 1):
            score_tmp = {m['theme_id']: [m['theme_name'], i / (len(m['theme_name']) + 0.5)] for m in
                         tb_jb[(tb_jb_theme.str[:i] == theme_name.replace(' ', '')[:i]) |
                               (tb_jb_theme.str[-i:] == theme_name.replace(' ', '')[-i:])].iloc}
            score_dict.update(score_tmp)
        return score_dict

    for br in tb_br.iloc:
        print(br['theme_name'])
        match_dict = check_match_score(br['theme_name'])
        brand, branch = br['brand_name'], br['branch_name'].split(' ')[0]
        for tid in match_dict:
            if match_dict[tid][1] < 0.2:
                continue
            cafe_name_tmp = tb_jb[tb_jb['theme_id'] == tid].iloc[0]['cafe_name']
            if brand in cafe_name_tmp and branch in cafe_name_tmp:
                # escape_brand_theme
                # TODO: match score 최신 값으로 비교
                execute_sql('select match_score from roomona.escape_brand_theme where theme_id = %s' % tid)
                if not br['match_score'] or br['match_score'] < match_dict[tid][1]:
                    update_brand_theme_id(tid, match_dict[tid][1], br['brand_num'], br['branch_num'], br['theme_num'])
    return

# 예약 테이블 업데이트

