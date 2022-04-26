import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from ETL.db_handle import execute_sql, upsert_brand_theme


def find_theme_keyescape():
    brand_num, brand_name = 0, '키이스케이프'
    # reserve_url = 'https://keyescape.co.kr/web/home.php?go=rev.make'
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
                # 'reserve_url': reserve_url,
            }
            result.append(res_dict)
    return result
# keyescape = find_theme_keyescape()


# def post_reservation_keyescape(theme_id, date_str, theme_num):
def post_reservation_keyescape(date_str, record):
    # sql = '''SELECT theme_num, theme_name
    # FROM roomona.escape_brand_theme
    # WHERE theme_id = %s''' % theme_id
    # selected = execute_sql(sql)
    url = 'https://keyescape.co.kr/web/rev.theme_time.php'
    headers = {'content-type': 'application/x-www-form-urlencoded; charset=utf-8'}
    data = {
        'rev_days': date_str,
        # 'theme_num': str(selected['theme_num'].values[0]),
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
        # 'theme': selected['theme_name'].values[0],
        'theme': record['theme_name'].values[0],
        'possible': possible,
        'impossible': impossible,
    }
    return result
# post_reservation_keyescape(142, '2022-04-30')


def find_theme_xphobia():
    brand_num, brand_name = 1, '비트포비아'
    # reserve_url = 'https://www.xphobia.net/reservation/reservation_check.php'
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
            if theme_name == '렛츠 플레이':
                theme_name += ' - LETS PLAY'
            res_dict = {
                'brand_num': brand_num,
                'brand_name': brand_name,
                'branch_num': branch_num,
                'branch_name': branch_name,
                'theme_num': theme_num,
                'theme_name': theme_name,
                # 'reserve_url': reserve_url,
            }
            result.append(res_dict)
    return result
# xphobia = find_theme_xphobia()


def post_reservation_xphobia(date_str, record):
    # sql = '''SELECT theme_num, theme_name, branch_name
    # FROM roomona.escape_brand_theme
    # WHERE theme_id = %s''' % theme_id
    # selected = execute_sql(sql)
    url = 'https://www.xphobia.net/reservation/ck_date2_no1.php'  # 두번 요청해야 함
    weekend = datetime.strptime(date_str, '%Y-%m-%d').weekday() >= 5
    data = {'shop': record['branch_name'].values[0],
            'quest': record['theme_name'].values[0],
            'quest2': record['theme_name'].values[0],
            'date': ''.join(date_str.split('-'))}
    res = requests.post(url, data=data).json()[0]
    week_str = 'ro_end' if weekend else 'ro_day'
    time_all = [res[week_str + str(i)].split('_')[0] for i in range(1, 50) if res[week_str + str(i)]]

    url = 'https://www.xphobia.net/reservation/ck_date_no1.php'
    data['time[]'] = time_all
    impossible_dict = requests.post(url, data=data).json()

    impossible = [j['rel_order_time'] for j in impossible_dict]
    possible = [t for t in time_all if t not in impossible]
    result = {
        'theme': record['theme_name'].values[0],
        'possible': possible,
        'impossible': impossible,
    }
    return result
# post_reservation_xphobia(3355, '2022-04-28')


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
            }
            result.append(res_dict)
    return result
# secretgarden = find_theme_secretgarden()


def get_reservation_secretgarden(date_str, record):
    # sql = '''SELECT theme_num, theme_name, branch_num
    # FROM roomona.escape_brand_theme
    # WHERE theme_id = %s''' % theme_id
    # selected = execute_sql(sql)
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
    # 강남점만 진행
    brand_num, brand_name, branch_num, branch_name = 3, '제로월드', 0, '강남점'
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
        }
        result.append(res_dict)
    return result
# zeroworld = find_theme_zeroworld()


def post_reservation_zeroworld(date_str, record):
    # sql = '''SELECT theme_num, theme_name, branch_num
    # FROM roomona.escape_brand_theme
    # WHERE theme_id = %s''' % theme_id
    # selected = execute_sql(sql)

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


# def find_theme_nextedition():
#     url_main = 'https://www.nextedition.co.kr/shops/'
#     res = requests.get(url_main)
#     soup = BeautifulSoup(res.text, 'html.parser')
#     # TODO: branch num이 숫자가 아님 - 보류(table에 request key 추가 후 비트포비아도 같이 수정하기)


def check_reservation(theme_id, date_str):
    sql = '''SELECT brand_num, brand_name, branch_num, branch_name, theme_num, theme_name
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


for theme_id in [2197, 3083, 2019, 2021, 3386, 3593, 3437, 3321]:
    for date_str in ['2022-04-27', '2022-04-28', '2022-04-29']:
        cr = check_reservation(theme_id, date_str)
        print(cr['theme'], date_str)
        print(cr['possible'])

