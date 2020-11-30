import pandas as pd
import numpy as np
import cx_Oracle as cx

# init只在初始运行一次，连接oracle用的
cx.init_oracle_client('/Users/rui/Downloads/instantclient_19_8')

#engine = create_engine("oracle+cx_oracle://de_jyzlpj:oracle@kf/?encoding=UTF-8&nencoding=UTF-8")
# cx_connector = 'de_jyzlpj/oracle@192.2.2.15:1521/kf'
# conn = cx.connect(cx_connector)
# cursor = conn.cursor()

# data = [{
#     'agg_jysc_dzyszb_cc': '5.83',
#     'agg_jysc_lrlcl_cc': '0',
#     'cycle_name': '202010',
#     'sc01_name2': '3301'
# }, {
#     'agg_jysc_dzyszb_cc': '3.8',
#     'agg_jysc_lrlcl_cc': '0',
#     'cycle_name': '202011',
#     'sc01_name2': '3301'
# }, {
#     'agg_jysc_dzyszb_cc': '2.9',
#     'agg_jysc_lrlcl_cc': '0.0345',
#     'cycle_name': '202012',
#     'sc01_name2': '3301'
# }, {
#     'agg_jysc_dzyszb_cc': '22.5',
#     'agg_jysc_lrlcl_cc': '0.0222',
#     'cycle_name': '202007',
#     'sc01_name2': '3301'
# }, {
#     'agg_jysc_dzyszb_cc': '2.28',
#     'agg_jysc_lrlcl_cc': '0',
#     'cycle_name': '202008',
#     'sc01_name2': '3301'
# }, {
#     'agg_jysc_dzyszb_cc': '0.85',
#     'agg_jysc_lrlcl_cc': '0',
#     'cycle_name': '202009',
#     'sc01_name2': '3301'
# }]


def getVarNorm(a_list):
    '''

    '''
    # 计算均值方差
    a_avg = np.average(a_list)
    a_std = np.std(a_list)
    # a_std = np.std(a_list)/np.sqrt(a_list.shape[0]-1)
    # #print('Avg:{}, Std:{}'.format(a_avg, a_std))
    # 标准化
    a_norm = (a_list-a_avg)/a_std
    # #print('{} Normalized:{}'.format(city, a_norm))
    # 对称变化率
    a1 = a_norm[:-1]
    # #print(a1)
    a2 = a_norm[1:]
    # #print(a2)
    a_var1 = a2 - a1
    # #print('对称变化率：{}'.format(a_var1))
    # 标准化
    a_var1_norm = (a_var1.shape[0]*a_var1)/np.sum(np.abs(a_var1))
    # print('标准化对称变化率：{}'.format(a_var1_norm))
    return a_var1_norm.tolist()


def result_jy(data):
    '''

    '''
    cx_connector = 'de_jyzlpj/oracle@192.2.2.15:1521/kf'
    conn = cx.connect(cx_connector)
    cursor = conn.cursor()

    # 获取单城市数据
    df = pd.DataFrame.from_records(data, columns=[
        'sc01_name2', 'cycle_name', 'agg_jysc_lrlcl_cc',
        'agg_jysc_dzyszb_cc']).sort_values(by=['sc01_name2', 'cycle_name'])
    # #print(df['sc01_name2'].unique())
    # pd.json_normalize
    city_code_list = df['sc01_name2'].unique()
    time_list = df['cycle_name'].unique()
    # print(city_code_list, time_list)
    # a for 流入流出，b for 大专以上占比
    a_weight = 2
    b_weight = 2

    market_score_April = {'3301': 83.39,
                          '3302':	79.79,
                          '3303':	77.28,
                          '3304':	77.45,
                          '3305':	80.93,
                          '3306':	83.31,
                          '3307':	75.95,
                          '3308':	84.76,
                          '3309':	79.20,
                          '3310':	79.21,
                          '3311':	78.71
                          }

    city_tran = {
        '3301': '杭州市',
        '3302': '宁波市',
        '3303': '温州市',
        '3304': '嘉兴市',
        '3305': '湖州市',
        '3306': '绍兴市',
        '3307': '金华市',
        '3308': '衢州市',
        '3309': '舟山市',
        '3310': '台州市',
        '3311': '丽水市'
    }

    dict = []  # 各城市分数

    for city in city_code_list:
        a0 = market_score_April[city]
        city_score = []
        city_score.append(a0)
        dict.append({
            "area": city,
            "qb": "202004",
            "score": a0
        })
        # a for 流入流出，b for 大专以上占比
        a_list_pd = df[df['sc01_name2'] == city]['agg_jysc_lrlcl_cc']
        a_list = pd.to_numeric(a_list_pd, downcast='float').to_numpy()
        b_list_pd = df[df['sc01_name2'] == city]['agg_jysc_dzyszb_cc']
        b_list = pd.to_numeric(b_list_pd, downcast='float').to_numpy()
    # 平均变化率
    # AHP合成变化率
        average_var1_norm = (a_weight*np.array(getVarNorm(a_list)) + b_weight*np.array(getVarNorm(b_list))) / (a_weight + b_weight)
        #print('平均变化率：{}'.format(average_var1_norm))

    # 计算月度分数（使用初始值和变化率）
        new_var = (200+average_var1_norm)/(200-average_var1_norm)
        for i in range(new_var.shape[0]):
            city_dict = {}  # 单组数据
            a0 = city_score[-1]
            city_score.append(a0*new_var[i])
            city_dict["area"] = city
            city_dict["qb"] = time_list[i+1]
            city_dict["score"] = city_score[i+1]
            # print('city_dict: {}'.format(city_dict))
            dict.append(city_dict)
            i += 1
        # print('city_score: {}'.format(city_score))
        # print(dict)

        # 单维度季度分数合成
        last_year = int(time_list[-1][:-2])
        # #print(last_year)
        last_month = int(time_list[-1][-2:])
        if last_month <= 3:
            last_season = 1
        elif last_month <= 6:
            last_season = 2
        elif last_month <= 9:
            last_season = 3
        else:
            last_season = 4
        # Season
        cur_year = last_year
        cur_season = last_season
        if last_month % 3 == 0:
            season_score = np.average([city_score[-1], city_score[-2], city_score[-3]])
        elif last_month % 3 == 1:
            season_score = np.average([city_score[-2], city_score[-3], city_score[-4]])
            if last_month == 1:
                cur_year -= 1
                cur_season = 4
            else:
                cur_season -= 1
        else:
            season_score = np.average([city_score[-3], city_score[-4], city_score[-5]])
            if last_month == 2:
                cur_year -= 1
                cur_season = 4
            else:
                cur_season -= 1
        city_name = city_tran[city]
        season_name = str(cur_year) + str(cur_season)
        # print('City {}, Season {}: {}'.format(city_name, season_name, season_score))

        # DB
        # print(f"UPDATE  JYCY  SET AAA001 = '{city_name}',AAA011 = {season_name},AAA004 = {season_score} where AAA001 = '{city_name}' and AAA011 = {season_name}")

        cursor.execute(f"select AAA004 from JYCY where AAA001='{city_name}' and AAA011 = {season_name}")

        # cursor.execute(f"select AAA005 from JYCY where AAA001='杭州市' and AAA011 = 20204")

        rows = cursor.fetchall()
        print('rows: {}'.format(rows))
        # print(f"UPDATE  JYCY  SET AAA001 = '{city_name}',AAA011 = {season_name},AAA004 = {season_score} where AAA001 = '{city_name}' and AAA011 = {season_name};")
        if rows != [] or rows == [(None,)]:
            # 更新季度数据
            cursor.execute(f"UPDATE  JYCY  SET AAA001 = '{city_name}',AAA011 = {season_name},AAA004 = {season_score} where AAA001 = '{city_name}' and AAA011 = {season_name}")
            print('Data updated successfully')
        else:
            # 插入新一季度数据
            cursor.execute(f"insert into JYCY (AAA001,AAA011,AAA004) VALUES ('{city_name}',{season_name},{season_score})")
            print('Data added successfully')
    cursor.close()
    conn.commit()
    conn.close()


if __name__ == '__main__':
    result_jy(data)
