import argparse
import sys
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, abs, mean, max, count, first, expr, substring, from_json, create_map, row_number
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, StringType, StructType, StructField, FloatType, DoubleType
from pyspark.ml.pipeline import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator

def feature_engineering(df):
    """
    피처 엔지니어링 함수: polars에서 PySpark로 변환된 버전
    
    Args:
        df: PySpark DataFrame
    
    Returns:
        df: 피처 엔지니어링이 완료된 PySpark DataFrame
    """
    # game_date 열에서 연도(년)만 잘라 year라는 새로운 열로 생성
    df = df.withColumn('year', substring('game_date', 1, 4))
    
    # 좌완 투수인 경우, 수평 무브먼트(ax) 방향을 반대로
    df = df.withColumn('ax', 
                      when(col('pitcher_hand') == 'L', -col('ax'))
                      .otherwise(col('ax')))
    
    # 좌완 투수의 릴리스 지점(x0)도 반대로 뒤집어줌
    df = df.withColumn('x0', 
                      when(col('pitcher_hand') == 'L', col('x0'))
                      .otherwise(-col('x0')))
    
    # 패스트볼 종류만 필터링
    pitch_types = ['SI', 'FF', 'FC']
    df_filtered = df.filter(col('pitch_type').isin(pitch_types))
    
    # 투수 ID와 연도를 기준으로 묶어서 패스트볼 관련 집계
    df_agg = df_filtered.groupBy('pitcher_id', 'year', 'pitch_type').agg(
        mean('start_speed').alias('avg_fastball_speed'),
        mean('az').alias('avg_fastball_az'),
        mean('ax').alias('avg_fastball_ax'),
        count('*').alias('count')
    )
    
    # 투구 수(count)와 평균 구속 기준으로 윈도우 함수를 사용해 정렬하고 
    # 각 투수-년도 조합별로 첫 번째 레코드만 선택
    window_spec = Window.partitionBy('pitcher_id', 'year').orderBy(col('count').desc(), col('avg_fastball_speed').desc())
    df_agg = df_agg.withColumn('rank', row_number().over(window_spec)).filter(col('rank') == 1).drop('rank')
    
    # 원본 데이터와 집계 데이터 조인
    df = df.join(df_agg, on=['pitcher_id', 'year'], how='left')
    
    # 패스트볼 정보가 없는 투수의 경우, 대체값 사용
    window_pitcher = Window.partitionBy('pitcher_id')
    df = df.withColumn('avg_fastball_speed', 
                      when(col('avg_fastball_speed').isNull(), max('start_speed').over(window_pitcher))
                      .otherwise(col('avg_fastball_speed')))
    
    df = df.withColumn('avg_fastball_az', 
                      when(col('avg_fastball_az').isNull(), max('az').over(window_pitcher))
                      .otherwise(col('avg_fastball_az')))
    
    df = df.withColumn('avg_fastball_ax', 
                      when(col('avg_fastball_ax').isNull(), max('ax').over(window_pitcher))
                      .otherwise(col('avg_fastball_ax')))
    
    # 현재 투구와 패스트볼 사이의 차이 계산
    df = df.withColumn('speed_diff', col('start_speed') - col('avg_fastball_speed'))
    df = df.withColumn('az_diff', col('az') - col('avg_fastball_az'))
    df = df.withColumn('ax_diff', abs(col('ax') - col('avg_fastball_ax')))
    
    # year 컬럼 정수형으로 변환
    df = df.withColumn('year', col('year').cast(IntegerType()))
    
    return df

def process_weekly_game_data(kafka_topic, bootstrap_servers, execution_date, output_path):
    """
    Spark 작업 메인 함수: 카프카에서 데이터를 읽고 처리하여 저장
    
    Args:
        kafka_topic (str): 카프카 토픽 이름
        bootstrap_servers (str): 카프카 서버 주소
        execution_date (str): 실행 날짜 (YYYY-MM-DD 형식)
        output_path (str): 처리 결과를 저장할 경로
    """
    # Spark 세션 생성
    spark = SparkSession.builder \
        .appName("MLB Weekly Game Data Processing") \
        .getOrCreate()
    
    # 로그 레벨 설정
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        # 실행 날짜 파싱
        exec_date = datetime.strptime(execution_date, '%Y-%m-%d')
        
        # 실행 날짜의 1주일 전부터 실행 날짜까지의 날짜 범위 계산
        start_date = (exec_date - timedelta(days=7)).strftime('%Y-%m-%d')
        end_date = (exec_date - timedelta(days=1)).strftime('%Y-%m-%d')
        
        print(f"처리 기간: {start_date} ~ {end_date}")
        
        # MLB API 게임 데이터 스키마 정의
        game_schema = StructType([
            StructField("gamePk", IntegerType(), True),
            StructField("gameDate", StringType(), True),
            StructField("teams", StructType([
                StructField("away", StructType([
                    StructField("team", StructType([
                        StructField("id", IntegerType(), True),
                        StructField("name", StringType(), True)
                    ]), True)
                ]), True),
                StructField("home", StructType([
                    StructField("team", StructType([
                        StructField("id", IntegerType(), True),
                        StructField("name", StringType(), True)
                    ]), True)
                ]), True)
            ]), True)
        ])
        
        # Kafka에서 데이터 읽기 (배치 모드)
        df_kafka = spark \
            .read \
            .format("kafka") \
            .option("kafka.bootstrap.servers", bootstrap_servers) \
            .option("subscribe", kafka_topic) \
            .option("startingOffsets", "earliest") \
            .option("endingOffsets", "latest") \
            .load()
        
        # kafka 메시지에서 값 추출 및 스키마 변환
        df = df_kafka.selectExpr("CAST(value AS STRING) as json_data")
        
        # JSON 파싱
        df = df.withColumn("parsed_data", from_json(col("json_data"), game_schema))
        df = df.select("parsed_data.*")
        
        # MLB 투구 데이터 로드
        mlb_pitch_data = spark.read.csv("mlb_pitch_data_2020_2024.csv", header=True, inferSchema=True)
        
        # run values 데이터 로드
        run_values_data = spark.read.csv("data/run_values.csv", header=True, inferSchema=True)
        
        # 투구 결과 사전 정의
        des_dict_broadcast = spark.sparkContext.broadcast({
            "Ball": "ball",
            "In play, run(s)": "hit_into_play",
            "In play, out(s)": "hit_into_play",
            "In play, no out": "hit_into_play",
            "Called Strike": "called_strike",
            "Foul": "foul",
            "Swinging Strike": "swinging_strike",
            "Blocked Ball": "ball",
            "Swinging Strike (Blocked)": "swinging_strike",
            "Foul Tip": "swinging_strike",
            "Foul Bunt": "foul",
            "Hit By Pitch": "hit_by_pitch",
            "Pitchout": "ball",
            "Missed Bunt": "swinging_strike",
            "Bunt Foul Tip": "swinging_strike",
            "Foul Pitchout": "foul",
            "Ball In Dirt": "ball"
        })
        
        # play_description 표준화 (UDF 사용)
        def standardize_description(description):
            if description in des_dict_broadcast.value:
                return des_dict_broadcast.value[description]
            return description
        
        # UDF 등록
        standardize_description_udf = spark.udf.register("standardize_description", standardize_description, StringType())
        
        # event_type, balls, strikes로 join
        mlb_joined = mlb_pitch_data.join(
            run_values_data,
            (mlb_pitch_data["event_type"] == run_values_data["event"]) &
            (mlb_pitch_data["balls"] == run_values_data["balls"]) &
            (mlb_pitch_data["strikes"] == run_values_data["strikes"]),
            "left"
        )
        
        # play_description 표준화
        mlb_joined = mlb_joined.withColumn(
            "play_description", 
            standardize_description_udf(col("play_description"))
        )
        
        # play_description, balls, strikes로 다시 join
        mlb_joined = mlb_joined.join(
            run_values_data.withColumnRenamed("delta_run_exp", "delta_run_exp_des"),
            (mlb_joined["play_description"] == run_values_data["event"]) &
            (mlb_joined["balls"] == run_values_data["balls"]) &
            (mlb_joined["strikes"] == run_values_data["strikes"]),
            "left"
        )
        
        # target 열에 Run Value(RV) 계산
        mlb_joined = mlb_joined.withColumn(
            "target",
            when(col("delta_run_exp").isNull(), col("delta_run_exp_des"))
            .otherwise(col("delta_run_exp"))
        )
        
        # 피처 엔지니어링 수행
        mlb_processed = feature_engineering(mlb_joined)
        
        # 모델링을 위한 특성(feature) 선택
        feature_cols = ["speed_diff", "az_diff", "ax_diff", "start_speed", "x0", "z0", "ax", "az", "vx0", "vy0", "vz0"]
        
        # 벡터 어셈블러 생성
        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
        
        # LightGBM 대신 GBTRegressor 사용
        gbr = GBTRegressor(featuresCol="features", labelCol="target", maxIter=100)
        
        # 파이프라인 생성
        pipeline = Pipeline(stages=[assembler, gbr])
        
        # 트레이닝 및 테스트 데이터 분할
        train_data, test_data = mlb_processed.randomSplit([0.8, 0.2], seed=42)
        
        # 모델 트레이닝
        model = pipeline.fit(train_data)
        
        # 모델 평가
        predictions = model.transform(test_data)
        evaluator = RegressionEvaluator(labelCol="target", predictionCol="prediction", metricName="rmse")
        rmse = evaluator.evaluate(predictions)
        
        # 결과 저장
        output_path_with_date = f"{output_path}/execution_date={execution_date}"
        
        predictions.write \
            .mode("overwrite") \
            .parquet(output_path_with_date)
        
        # 모델 저장
        model.write().overwrite().save(f"{output_path_with_date}/model")
        
        # 작업 성공 로그
        print(f"{start_date} ~ {end_date} 기간 데이터 처리 완료. 모델 RMSE: {rmse}")
        
    except Exception as e:
        print(f"process_weekly_game_data 오류 발생: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='주간 MLB 경기 데이터 처리')
    parser.add_argument('--kafka_topic', required=True, help='데이터를 읽을 Kafka 토픽')
    parser.add_argument('--bootstrap_servers', required=True, help='Kafka 부트스트랩 서버')
    parser.add_argument('--execution_date', required=True, help='실행 날짜 (YYYY-MM-DD)')
    parser.add_argument('--output_path', required=True, help='처리 결과 저장 경로')
    
    args = parser.parse_args()
    
    process_weekly_game_data(
        args.kafka_topic,
        args.bootstrap_servers,
        args.execution_date,
        args.output_path
    )