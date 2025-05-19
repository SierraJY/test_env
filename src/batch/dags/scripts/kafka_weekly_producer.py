import json
import requests
from datetime import datetime, timedelta
from kafka import KafkaProducer
import logging

def kafka_weekly_producer(kafka_topic, bootstrap_servers, execution_date):
    """
    1주일 전 경기 데이터를 가져와 카프카 토픽에 전송하는 함수
    
    Args:
        kafka_topic (str): 카프카 토픽 이름
        bootstrap_servers (str): 카프카 서버 주소
        execution_date (str): 실행 날짜 (YYYY-MM-DD 형식)
    """
    logging.info(f"Starting kafka_weekly_producer for execution_date: {execution_date}")
    
    # 실행 날짜 파싱
    exec_date = datetime.strptime(execution_date, '%Y-%m-%d')
    
    # 실행 날짜의 1주일 전부터 실행 날짜까지의 날짜 범위 계산
    # Airflow가 매주 월요일 실행되므로, 이전 월요일부터 일요일까지의 데이터를 가져옴
    start_date = (exec_date - timedelta(days=7)).strftime('%Y-%m-%d')
    end_date = (exec_date - timedelta(days=1)).strftime('%Y-%m-%d')
    
    logging.info(f"MLB 경기 데이터 수집 기간: {start_date} ~ {end_date}")
    
    # MLB API에서 데이터 가져오기
    try:
        # MLB API 데이터 가져오기
        response = requests.get(
            f"https://statsapi.mlb.com/api/v1/schedule",
            params={
                "startDate": start_date,
                "endDate": end_date,
                "sportId": 1,  # MLB
                "hydrate": "game,team"
            },
            timeout=30
        )
        response.raise_for_status()
        games_data = response.json()
        
        # 카프카 프로듀서 초기화
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        # 각 게임 데이터를 카프카에 전송
        game_count = 0
        for date in games_data.get('dates', []):
            for game in date.get('games', []):
                # 게임 데이터 전송
                producer.send(kafka_topic, value=game)
                game_count += 1
        
        # 모든 메시지가 전송될 때까지 대기
        producer.flush()
        producer.close()
        
        logging.info(f"Kafka 토픽({kafka_topic})으로 {game_count}개 경기 데이터 전송 완료")
        return f"{start_date} ~ {end_date} 기간의 {game_count}개 경기 처리 완료"
    
    except Exception as e:
        logging.error(f"kafka_weekly_producer 오류 발생: {str(e)}")
        raise