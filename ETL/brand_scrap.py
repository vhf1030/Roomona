import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from ETL.db_handle import execute_sql


def find_theme_keyescape():
    brand_num, brand_name = 0, '키이스케이프'
    url = 'https://keyescape.co.kr/web/home.php?go=rev.make'
    headers = {'content-type': 'text/html; charset=utf-8'}
    res = requests.get(url, headers=headers)
    res.encoding = 'utf-8'
    soup = BeautifulSoup(res.text, 'html.parser')
    zizum_tag = soup.select_one('#zizum_data').find_all('a')
    branch_dict = {}
    for zt in zizum_tag:
        branch_num = zt['href'].split("'")[1]
        branch_name = zt.find('li').text
        branch_dict[branch_num] = branch_name

    url = 'https://keyescape.co.kr/web/rev.theme.php'
    headers = {'content-type': 'application/x-www-form-urlencoded; charset=utf-8'}
    date_str = datetime.now().strftime('%Y-%m-%d')
    result = []
    for k in branch_dict:
        data = {
            'zizum_num': str(k),
            'rev_days': date_str,
        }
        res = requests.post(url, headers=headers, data=data)
        res.encoding = 'utf-8'
        soup = BeautifulSoup(res.text, 'html.parser')
        for a in soup.find_all('a'):
            theme_num = a['href'].split("'")[1]
            theme_name = a.find('li').text
            res_dict = {
                'brand_num': brand_num,
                'brand_name': brand_name,
                'branch_num': k,
                'branch_name': branch_dict[k],
                'theme_num': theme_num,
                'theme_name': theme_name,
                'request_key': None,
            }
            result.append(res_dict)
    return result
# keyescape = find_theme_keyescape()


# def post_reservation_keyescape(theme_id, date_str, theme_num):
def post_reservation_keyescape(date_str, record):
    url = 'https://keyescape.co.kr/web/rev.theme_time.php'
    headers = {'content-type': 'application/x-www-form-urlencoded; charset=utf-8'}
    data = {
        'rev_days': date_str,
        'theme_num': str(record['theme_num'].values[0]),
    }
    res = requests.post(url, headers=headers, data=data)
    soup = BeautifulSoup(res.text, 'html.parser')

    possible, impossible = [], []
    for li in soup.find_all('li'):
        if 'possible' in li['class']:
            possible.append(li.text.strip())
        else:
            impossible.append(li.text.strip())
    result = {
        'theme': record['theme_name'].values[0],
        'possible': possible,
        'impossible': impossible,
    }
    return result
# post_reservation_keyescape(142, '2022-04-30')
# check_reservation(142, '2022-04-30')


def find_theme_xphobia():
    brand_num, brand_name = 1, '비트포비아'
    date = (datetime.now() + timedelta(days=1)).strftime('%Y%m%d')
    url = 'https://www.xphobia.net/reservation/ck_no1.php'
    data = {'cate': '포비아 던전',  # 방탈출 카페도 동일하지만 생략함
            'date': date}
    res = requests.post(url, data=data)
    result = []
    for j1 in res.json():
        branch_num, branch_name = j1['ji_id'], j1['ji_name']
        url = 'https://www.xphobia.net/reservation/ck_quest_no1.php'
        data = {'shop': j1['ji_name'],
                'date': date}
        res = requests.post(url, data=data)
        for j2 in res.json():
            theme_num, theme_name = j2['ro_id'], j2['ro_name']
            # if theme_name == '렛츠 플레이':
            #     theme_name += ' - LETS PLAY'  # 예약 확인 시 테마명 그대로 필요함
            # branch_name_tmp = tb['branch_name'].replace('Ⅱ', 'II')
            res_dict = {
                'brand_num': brand_num,
                'brand_name': brand_name,
                'branch_num': branch_num,
                'branch_name': branch_name.replace('Ⅱ', 'II'),  # jb와 동일하게 변경
                'theme_num': theme_num,
                'theme_name': theme_name,
                'request_key': branch_name,
            }
            result.append(res_dict)
    return result
# xphobia = find_theme_xphobia()


def post_reservation_xphobia(date_str, record):
    url = 'https://www.xphobia.net/reservation/ck_date2_no1.php'  # 두번 요청해야 함
    weekend = datetime.strptime(date_str, '%Y-%m-%d').weekday() >= 5
    data = {'shop': record['request_key'].values[0],
            'quest': record['theme_name'].values[0],
            'quest2': record['theme_name'].values[0],
            'date': ''.join(date_str.split('-'))}
    res = requests.post(url, data=data).json()[0]
    week_str = 'ro_end' if weekend else 'ro_day'
    time_all = [res[week_str + str(i)].split('_')[0] for i in range(1, 50) if res[week_str + str(i)]]

    url = 'https://www.xphobia.net/reservation/ck_date_no1.php'
    data['time[]'] = time_all
    impossible_dict = requests.post(url, data=data).json()

    impossible = sorted([j['rel_order_time'] for j in impossible_dict])
    possible = sorted([t for t in time_all if t not in impossible])
    result = {
        'theme': record['theme_name'].values[0],
        'possible': possible,
        'impossible': impossible,
    }
    return result
# post_reservation_xphobia(3355, '2022-04-28')
# check_reservation(3355, '2022-04-30')
# check_reservation(3380, '2022-04-30')


def find_theme_secretgarden():
    brand_num, brand_name = 2, '비밀의화원'
    url = 'http://www.secretgardenescape.com/reservation.html'
    headers = {'content-type': 'text/html; charset=utf-8'}
    res = requests.get(url, headers=headers)
    soup = BeautifulSoup(res.text, 'html.parser')
    branch_tag = soup.select_one('div.tabList').find_all('a')
    branch_dict = {}
    for bt in branch_tag:
        branch_num = bt['href'].split("=")[1]
        branch_name = bt.text
        branch_dict[branch_num] = branch_name

    url = 'http://www.secretgardenescape.com/'
    headers = {'content-type': 'text/html; charset=utf-8'}
    res = requests.get(url, headers=headers)
    soup = BeautifulSoup(res.text, 'html.parser')
    result = []
    for tc in soup.select('div.tabCont'):
        for li in tc.find_all('li'):
            theme_name = li.find(class_='txt_01').text
            if theme_name == 'Promesa':
                theme_name += ' - 프로메사'
            if theme_name == '삼남매 이씨':
                theme_name += '(장녀)'  # 중복을 피하기 위해 하나로 통합
            a_split = li.find('a')['href'].split('&')
            branch_num = a_split[0].split('=')[1]
            theme_num = a_split[1].split('=')[1]
            res_dict = {
                'brand_num': brand_num,
                'brand_name': brand_name,
                'branch_num': branch_num,
                'branch_name': branch_dict[branch_num],
                'theme_num': theme_num,
                'theme_name': theme_name,
                'request_key': None,
            }
            result.append(res_dict)
    return result
# secretgarden = find_theme_secretgarden()


def get_reservation_secretgarden(date_str, record):
    url = ('http://www.secretgardenescape.com/reservation.html?' +
           'k_shopno=' + str(record['branch_num'].values[0]) +
           '&rdate=' + date_str +
           '&prdno=' + str(record['theme_num'].values[0]))
    res = requests.get(url)
    soup = BeautifulSoup(res.text, 'html.parser')
    impossible, possible = [], []
    for li in soup.find(class_='reserve_Time').find_all('li'):
        time = li.find('span').text.strip()
        if 'onclick' in li.find('a').attrs:
            possible.append(time)
        else:
            impossible.append(time)
    result = {
        'theme': record['theme_name'].values[0],
        'possible': possible,
        'impossible': impossible,
    }
    return result
# get_reservation_secretgarden(3295, '2022-04-26')


def find_theme_zeroworld():
    brand_num, brand_name, branch_num, branch_name = 3, '제로월드', 0, '강남점'  # 강남점만 진행
    # global z_session
    url = 'https://www.zerogangnam.com/theme'
    res = requests.get(url)
    soup = BeautifulSoup(res.text, 'html.parser')
    result = []

    for tp in soup.find(class_='themepop').find_all('section'):
        theme_name = tp.find('h2').text.split('] ')[1]
        theme_num = tp['data-no']
        res_dict = {
            'brand_num': brand_num,
            'brand_name': brand_name,
            'branch_num': branch_num,
            'branch_name': branch_name,
            'theme_num': theme_num,
            'theme_name': theme_name,
            'request_key': None,
        }
        result.append(res_dict)
    return result
# zeroworld = find_theme_zeroworld()


def post_reservation_zeroworld(date_str, record):
    # get session
    url = 'https://www.zerogangnam.com/theme'
    res = requests.get(url)
    soup = BeautifulSoup(res.text, 'html.parser')
    csrf = soup.find(id='csrf')['content']
    session = {
        'xsrf': res.cookies['XSRF-TOKEN'],
        'session': res.cookies['_session'],
        'csrf': csrf
    }

    url = 'https://www.zerogangnam.com/reservation/theme'
    headers = {
        'accept': 'application/json, text/javascript',
        'accept-encoding': 'gzip, deflate, br',
        'content-type': 'application/x-www-form-urlencoded; charset=utf-8',
        'cookie': 'XSRF-TOKEN=' + session['xsrf'] + '; _session=' + session['session'],
        'x-csrf-token': session['csrf'],
    }
    data = {'reservationDate': date_str}
    res = requests.post(url, headers=headers, data=data)
    time_dict = res.json()['times']
    theme_num_tmp = (str(record['theme_num'].values[0])
                     if str(record['theme_num'].values[0]) in time_dict
                     else str(int(record['theme_num'].values[0])+1))
    result_dict = time_dict[theme_num_tmp]
    impossible, possible = [], []
    for rd in result_dict:
        if rd['reservation']:
            impossible.append(':'.join(rd['time'].split(':')[:-1]))
        else:
            possible.append(':'.join(rd['time'].split(':')[:-1]))
    result = {
        'theme': record['theme_name'].values[0],
        'possible': possible,
        'impossible': impossible,
    }
    return result
# post_reservation_zeroworld(3437, '2022-04-26')


def find_theme_nextedition():
    brand_num, brand_name = 4, '넥스트에디션'
    url_main = 'https://www.nextedition.co.kr/shops/'
    res = requests.get(url_main)
    soup = BeautifulSoup(res.text, 'html.parser')
    # url 확인
    rk_list = []
    for a in soup.find(class_='modal-body').find_all('a'):
        rk = a['href'].split('/')[-1]
        for cafe_filt in ['gangnam', 'gundae', 'sinchon', 'jamsil']:
            if cafe_filt in rk.lower():
                rk_list.append(a['href'].split('/')[-1])

    result = []
    for rk in rk_list:
        url = url_main + rk
        res = requests.get(url)
        soup = BeautifulSoup(res.text, 'html.parser')
        branch_name = soup.h1.text.split('넥스트에디션 ')[1]
        for theme_tag in soup.find(class_='themes').find_all(class_='white-page-content'):
            theme_name = theme_tag.h2.text.strip()
            branch_num, theme_num = theme_tag.find(class_='res-btn')['onclick'].split('(')[1].split(')')[0].split(', ')[:2]
            res_dict = {
                'brand_num': brand_num,
                'brand_name': brand_name,
                'branch_num': branch_num,
                'branch_name': branch_name,
                'theme_num': theme_num,
                'theme_name': theme_name,
                'request_key': rk,
            }
            result.append(res_dict)
    return result
            # time_dict = {}
            # for res_tag in theme_tag.find_all(class_='res-btn'):
            #     cafe_theme_time = res_tag['onclick'].split('(')[1].split(')')[0].split(', ')
            #     time_dict[cafe_theme_time[2]] = res_tag.find(class_='time').text.strip()
            # cafe_num = cafe_theme_time[0]
            # print('넥스트에디션', theme)
            # if theme in result:
            #     print('theme duplicated error', theme, result[theme], cafe_name)
            # result[theme] = (cafe_num, time_dict)


def get_reservation_nextedition(date_str, record):
    # 두번 요청해야 함
    url = 'https://www.nextedition.co.kr/shops/' + record['request_key'].values[0]
    res = requests.get(url)
    soup = BeautifulSoup(res.text, 'html.parser')
    for theme_tag in soup.find(class_='themes').find_all(class_='white-page-content'):  # 전체 테마 예약정보가 한 페이지에 있음
        theme_num = theme_tag.find(class_='res-btn')['onclick'].split('(')[1].split(')')[0].split(', ')[1]
        if int(theme_num) == record['theme_num'].values[0]:
            time_dict = {}
            for res_tag in theme_tag.find_all(class_='res-btn'):
                cafe_theme_time = res_tag['onclick'].split('(')[1].split(')')[0].split(', ')
                time_dict[cafe_theme_time[2]] = res_tag.find(class_='time').text.strip()

    url = 'https://www.nextedition.co.kr/reservation_info?date=' + date_str + '&shop=' + str(record['branch_num'].values[0])
    res = requests.get(url, headers={'x-requested-with': 'XMLHttpRequest'})
    impossible_num = res.text.split('[')[1].split(']')[0].split(', ')
    impossible, possible = [], []
    for time_num in time_dict:
        if time_num in impossible_num:
            impossible.append(time_dict[time_num])
        else:
            possible.append(time_dict[time_num])
    result = {
        'theme': record['theme_name'].values[0],
        'possible': possible,
        'impossible': impossible,
    }
    return result


def check_reservation(theme_id, date_str):
    sql = '''SELECT brand_num, brand_name, branch_num, branch_name, theme_num, theme_name, request_key
    FROM roomona.escape_brand_theme
    WHERE theme_id = %s''' % theme_id
    selected = execute_sql(sql)
    if selected['brand_num'].values[0] == 0:
        return post_reservation_keyescape(date_str, selected)
    if selected['brand_num'].values[0] == 1:
        return post_reservation_xphobia(date_str, selected)
    if selected['brand_num'].values[0] == 2:
        return get_reservation_secretgarden(date_str, selected)
    if selected['brand_num'].values[0] == 3:
        return post_reservation_zeroworld(date_str, selected)
    if selected['brand_num'].values[0] == 4:
        return get_reservation_nextedition(date_str, selected)

# for theme_id in [2197, 3083, 2019, 3386, 3593, 3437, 3159, 3498]:
#     for date_str in ['2022-04-30', '2022-05-01', '2022-05-02', '2022-05-03', '2022-05-04']:
#         cr = check_reservation(theme_id, date_str)
#         print(cr['theme'], date_str)
#         print(cr['possible'])


