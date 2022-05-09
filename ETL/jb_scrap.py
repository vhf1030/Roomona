import requests
import json
import re
from bs4 import BeautifulSoup
from datetime import datetime
from config.my_config import JB_PARAMS


def post_cafe_list_jb(location_category):
    url = 'https://www.roomescape.co.kr/bbs/ajax.search.company.list.v2.php'
    headers = JB_PARAMS['headers']
    data = JB_PARAMS['data']['cafe_list']
    data['search_select_loc'] = '["' + location_category + '"]'
    res = requests.post(url, headers=headers, data=data)
    result_dict = json.loads(res.json())
    cafe_list = []
    for c_raw in result_dict['List']:  # parsing 진행하여 저장
        cafe_tmp = {
            'cafe_id': c_raw['msg1'],
            'cafe_name': c_raw['msg3'],
            'location_category': location_category,
            'location_detail': c_raw['msg7'],
            'star_rate': c_raw['msg5'],
            'star_num': c_raw['msg12'],
            # 'home_url': None,  # update time 이용하여 incremental update 진행
        }
        cafe_list.append(cafe_tmp)
    return cafe_list
# pcl = post_cafe_list_jb('전국')  # 전국: 353 / 서울: 143 / 강남: 36


def post_theme_list_jb(location_category):
    url = 'https://www.roomescape.co.kr/bbs/ajax.search.theme.list.v2.php'
    headers = JB_PARAMS['headers']
    data = JB_PARAMS['data']['theme_list']
    data['search_select_loc'] = '["' + location_category + '"]'
    res = requests.post(url, headers=headers, data=data)
    result_dict = json.loads(res.json())
    theme_list = []
    for t_raw in result_dict['List']:  # parsing 진행하여 저장
        theme_tmp = {
            'theme_id': t_raw['msg1'],
            'theme_name': t_raw['msg4'],
            'cafe_id': t_raw['msg2'],
            'cafe_name': t_raw['msg5'],
            'star_rate': t_raw['msg7'],
        }
        theme_list.append(theme_tmp)
    return theme_list
# ptl = post_theme_list_jb('전국')  # 전국: 1700 / 서울: 602 / 강남: 141


# filtering 이후 진행 (테마평점 3점 이상)
def get_cafe_url_jb(cafe_id):
    url = 'https://www.roomescape.co.kr/store/detail.php?cafe=' + str(cafe_id)
    res = requests.get(url)
    soup = BeautifulSoup(res.text, 'html.parser')

    home_url_tag = soup.select('body > div.container > div.container_inner.section.section_def_info > div > div > div.def_info > div > div.reserve_n_contact > div > div.to_homepage > span > a')
    if len(home_url_tag) != 1:
        print('warning: home_url_tag')
    home_url = home_url_tag[0]['href']
    result = {
        'cafe_id': cafe_id,
        'home_url': home_url,
    }
    return result
# gcu = get_cafe_url_jb(716)


def parse_theme_meta(theme_id, theme_info_tag):
    limit_time = int(theme_info_tag.find(class_='def_info_theme_time').find(class_="value").text.split('분')[0])
    genre = theme_info_tag.find(class_='def_info_theme_genre_inner').find(class_="value").text.strip()
    difficulty = theme_info_tag.find(class_='def_info_theme_difficulty').find('img')['alt']
    lock_type_tag = theme_info_tag.find(class_='def_info_theme_device').find_all('img')
    lock_type = [t['src'].split('/')[-1].split('_')[0] for t in lock_type_tag if
                 t['src'].split('/')[-1].split('_')[1] == 'on.png'][0]
    activity_tag = theme_info_tag.find(class_='def_info_theme_activity').find_all('img')
    activity = [t['src'].split('/')[-1].split('_')[0] for t in activity_tag if
                t['src'].split('/')[-1].split('_')[1] == 'on.png'][0]
    recom_user_tag = theme_info_tag.find(class_='def_info_theme_allow_user').find_all('img')
    recom_user = ','.join([t['src'].split('/')[-1].split('_')[0] for t in recom_user_tag])
    metadata = {
        'theme_id': theme_id,
        'limit_time': limit_time,
        'genre': genre,
        'difficulty': difficulty,
        'lock_type': lock_type,
        'activity': activity,
        'recom_user': recom_user,
    }
    return metadata


def parse_theme_review(theme_id, theme_review_tag):
    # 기준 날짜 추가 예정
    review_stat_list = []
    review_text_list = []
    for rt in theme_review_tag:
        if rt['class'][0] != 'memb_review_box':
            continue
        writer_tag = rt.find(class_='writer')
        user_name = writer_tag.find(class_='name').find('a').text
        medal_img = writer_tag.find(class_='penticle').find('img')
        medal = medal_img['src'].split('/')[-1].split('.png')[0] if medal_img else None
        difficulty_img = writer_tag.find(class_='level').find('img')
        difficulty = difficulty_img['src'].split('difficulty_')[-1].split('.png')[0]
        success = 1 if writer_tag.find(class_='experi').find('span').text == '성공' else 0
        star_text_tag = rt.find(class_='star').find(class_='text')
        star = star_text_tag.text if star_text_tag else None
        span_tags = rt.find(class_='review_write_datetime').find_all('span')
        date_tmp, left_time, hint = int(span_tags.pop(0).text), '', ''
        while True:
            try:
                review_date = datetime.strptime(str(date_tmp), '%Y%m%d')
                break
            except ValueError:  # 날짜가 잘못 입력된 경우가 있음 20220230 등
                date_tmp -= 1
                if date_tmp < 19000000:
                    break
        while span_tags:
            st = span_tags.pop(0)
            if '남은시간' in st.text:
                left_time = span_tags.pop(0).text  # 초단위 생략
            if '힌트사용' in st.text:
                hint = span_tags.pop(0).text
        review_stat_tmp = {
            'theme_id': theme_id,
            'review_date': review_date,  # if int(date[4:6]) and int(date[6:8]) else None,
            'user_name': user_name,
            'medal': int(medal[-1]) if medal else None,  # 없는 경우도 있음 - 방수와 상관 없어보임
            'difficulty': (3 if difficulty == 'normal' else
                           3 + (-1 if 'easy' in difficulty else 1) * (2 if 'very' in difficulty else 1)),
            'success': success,
            'star': float(star) if star else None,
            'left_time': int(left_time.split('분')[0]) if re.compile("[0-9]+분").match(left_time) else None,
            'hint': int(hint.split('개')[0][-1]) if re.compile("[0-9]+개").match(hint) else None,
        }
        review_txt = rt.find(class_='review_bottom').find('span').text.strip()
        review_txt_tmp = {
            'theme_id': theme_id,
            'review_date': review_date,  # if int(date[4:6]) and int(date[6:8]) else None,
            'user_name': user_name,
            'review_txt': review_txt if review_txt else None,
        }
        review_stat_list.append(review_stat_tmp)
        review_text_list.append(review_txt_tmp)
    result = {
        'review_stat_list': review_stat_list,
        'review_text_list': review_text_list
    }
    return result


def get_theme_detail_jb(theme_id):
    url = 'https://www.roomescape.co.kr/theme/detail.php?theme=' + str(theme_id)
    res = requests.get(url)
    soup = BeautifulSoup(res.text, 'html.parser')

    theme_info_tag = soup.select_one('body > div.container > div.container_inner.section_detail.section_detail_def_info > div > div > div > div.def_info_box > div.def_info_text_box > div > div.def_info_text_box_inner_2')
    metadata = parse_theme_meta(theme_id, theme_info_tag)

    theme_review_tag = soup.select('#review > div')[3:]
    theme_review = parse_theme_review(theme_id, theme_review_tag)

    result = {
        'metadata': metadata,
        'review_stats': theme_review['review_stat_list'],
        'review_texts': theme_review['review_text_list'],
    }
    return result
# gtd = get_theme_detail_jb(3475)
# gtd['metadata']
# gtd['review_stats'][0]
# gtd['review_texts'][0]

