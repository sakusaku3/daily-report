import pandas as pd
from datetime import datetime
from datetime import timedelta
import json
from itertools import groupby

def get_config():
    with open('config.json', encoding='utf-8') as f:
        config = json.load(f)
    return config

def get_summary_type(config, project, category):
    for item in config:
        if item['category'] == project and item['detail'] == category:
            return item['summary']
        
    # 例外を発生させる
    raise Exception(f'設定が見つかりません{project},{category}')

def calculate_task_time(df):
    df['タスク時間'] = df.apply(
        lambda row: (datetime.combine(datetime.today(), row['終了']) - datetime.combine(datetime.today(), row['開始'])), axis=1)
    return df['タスク時間'].dt.total_seconds()

def group_by_month_and_summary(df, total_seconds):
    return total_seconds.groupby([df['月'], df['まとめ']]).sum()

def create_summary_df(summary, result):
    new_summary = pd.DataFrame(columns=summary.columns)
    for key, group in groupby(result.items(), lambda x: x[0][0]):
        mylist = []
        mylist.append(['月', key])
        for name, value in group:
            mylist.append([name[1], str(timedelta(seconds=int(value)))])
        dict_data = dict(mylist)
        new_summary = pd.concat([new_summary, pd.DataFrame([dict_data])], ignore_index=True)
    return new_summary

def main():
    try:
        # 設定ファイルを読み込む
        config = get_config()
        
        # CSVファイルを読み込む
        df = pd.read_csv('schedule.csv')
        summary = pd.read_csv('summary.csv')
        
        # 開始時間と終了時間をdatetime型に変換
        df['開始'] = pd.to_datetime(df['開始'], format='%H:%M').dt.time
        df['終了'] = pd.to_datetime(df['終了'], format='%H:%M').dt.time
        
        df['月日'] = pd.to_datetime(df['月日'])
        df['月'] = df['月日'].dt.month
        df['まとめ'] = df.apply(lambda row: get_summary_type(config, row['プロジェクト'], row['分類']), axis=1)
        
        # タスク時間を計算
        total_seconds = calculate_task_time(df)
        
        # プロジェクトと分類でグループ化し、タスク時間を合計
        result = group_by_month_and_summary(df, total_seconds)

        new_summary = create_summary_df(summary, result)

        new_summary.to_csv('summary.csv', index=False)
    except Exception as e:
        print(f"エラーが発生しました: {e}")

if __name__ == "__main__":
    main()