import pandas as pd
import requests
import time
from datetime import datetime, timedelta
from app.db.supabase import supabase
import numpy as np
from app.core.config import settings
from app.services.balance_service import get_overseas_balance

# 한국어 주식명과 티커 심볼 매핑
STOCK_TO_TICKER = {
    "애플": "AAPL",
    "마이크로소프트": "MSFT",
    "아마존": "AMZN",
    "구글 A": "GOOGL",
    "구글 C": "GOOG",
    "메타": "META",
    "테슬라": "TSLA",
    "엔비디아": "NVDA",
    "코스트코": "COST",
    "넷플릭스": "NFLX",
    "페이팔": "PYPL",
    "인텔": "INTC",
    "시스코": "CSCO",
    "컴캐스트": "CMCSA",
    "펩시코": "PEP",
    "암젠": "AMGN",
    "허니웰 인터내셔널": "HON",
    "스타벅스": "SBUX",
    "몬델리즈": "MDLZ",
    "마이크론": "MU",
    "브로드컴": "AVGO",
    "어도비": "ADBE",
    "텍사스 인스트루먼트": "TXN",
    "AMD": "AMD",
    "어플라이드 머티리얼즈": "AMAT",
    "S&P 500 ETF": "SPY",
    "QQQ ETF": "QQQ"
}

class StockRecommendationService:
    def __init__(self):
        # ETF 제외한 컬럼명 리스트
        self.stock_columns = list(STOCK_TO_TICKER.keys())[:-2]
        self.lookback_days = 180  # 6개월 데이터

    def calculate_sma(self, series, period):
        """단순 이동평균(SMA) 계산"""
        return series.rolling(window=period).mean()

    def calculate_ema(self, series, period):
        """지수 이동평균(EMA) 계산"""
        return series.ewm(span=period, adjust=False).mean()

    def calculate_rsi(self, series, period=14):
        """RSI 계산"""
        delta = series.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        return rsi

    def calculate_macd(self, series, short_period=12, long_period=26, signal_period=9):
        """MACD 및 Signal 라인 계산"""
        short_ema = self.calculate_ema(series, short_period)
        long_ema = self.calculate_ema(series, long_period)
        macd = short_ema - long_ema
        signal = self.calculate_ema(macd, signal_period)
        return macd, signal

    def generate_technical_recommendations(self):
        """기술적 지표를 기반으로 추천 데이터를 생성하고 Supabase에 저장"""
        # 최근 6개월 데이터만 가져오기
        end_date = datetime.now()
        start_date = end_date - timedelta(days=self.lookback_days)
        start_date_str = start_date.strftime("%Y-%m-%d")

        # Supabase 쿼리에서 컬럼명에 큰따옴표 추가
        quoted_columns = [f'"{col}"' for col in self.stock_columns]
        quoted_columns.append('"날짜"')  # 날짜 컬럼 추가

        response = supabase.table("economic_and_stock_data") \
            .select(*quoted_columns) \
            .gte("날짜", start_date_str) \
            .order("날짜") \
            .execute()

        if not response.data:
            return {"message": "데이터가 없습니다", "data": []}

        # 데이터프레임 생성 (컬럼명은 큰따옴표 제외)
        df = pd.DataFrame(response.data)
        df["날짜"] = pd.to_datetime(df["날짜"])
        df.set_index("날짜", inplace=True)
        df = df.astype(float)

        recommendations = []
        for stock in self.stock_columns:
            prices = df[stock]

            # 지표 계산
            sma20 = self.calculate_sma(prices, 20)
            sma50 = self.calculate_sma(prices, 50)
            golden_cross = sma20 > sma50
            rsi = self.calculate_rsi(prices)
            macd, signal = self.calculate_macd(prices)
            macd_buy_signal = macd > signal
            recommended = golden_cross & (rsi < 50) & macd_buy_signal

            # 가장 최근 날짜의 결과만 저장
            latest_date = df.index[-1]
            if all(pd.notna([sma20[latest_date], sma50[latest_date], rsi[latest_date], macd[latest_date], signal[latest_date]])):
                recommendations.append({
                    "날짜": latest_date.strftime("%Y-%m-%d"),
                    "종목": stock,
                    "SMA20": float(sma20[latest_date]),
                    "SMA50": float(sma50[latest_date]),
                    "골든_크로스": bool(golden_cross[latest_date]),
                    "RSI": float(rsi[latest_date]),
                    "MACD": float(macd[latest_date]),
                    "Signal": float(signal[latest_date]),
                    "MACD_매수_신호": bool(macd_buy_signal[latest_date]),
                    "추천_여부": bool(recommended[latest_date])
                })

        # 기존 데이터 삭제 후 새 데이터 저장
        try:
            # 전체 데이터 삭제 (항상 TRUE인 조건 사용)
            supabase.table("stock_recommendations").delete().eq("날짜", "1900-01-01").gte("날짜", "1900-01-01").execute()
            
            # 또는 이런 방식도 가능합니다 (모든 레코드와 매치되는 조건)
            supabase.table("stock_recommendations").delete().gte("날짜", "1900-01-01").execute()
            
            # 새 데이터 삽입
            supabase.table("stock_recommendations").insert(recommendations).execute()
        
        except Exception as e:
            print(f"오류 발생: {str(e)}")
            import traceback
            print(traceback.format_exc())  # 상세 스택 트레이스 출력
            raise Exception(f"추천 주식 분석 중 오류: {str(e)}")

        return {"message": f"{len(recommendations)}개의 추천 데이터가 생성되었습니다", "data": recommendations}

    def get_stock_recommendations(self):
        """
        Accuracy가 80% 이상이고 상승 확률이 3% 이상인 추천 주식 목록을 반환합니다.
        상승 확률 기준으로 내림차순 정렬됩니다.
        """
        response = supabase.table("stock_analysis_results").select("*").order("created_at", desc=True).execute()
        if not response.data:
            return {"message": "분석 결과를 찾을 수 없습니다", "recommendations": []}

        df = pd.DataFrame(response.data)
        numeric_columns = ['Accuracy (%)', 'Rise Probability (%)']
        for col in numeric_columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

        filtered_df = df[(df['Accuracy (%)'] >= 80) & (df['Rise Probability (%)'] >= 3)]
        filtered_df = filtered_df.sort_values(by='Rise Probability (%)', ascending=False)
        result_columns = [
            'Stock', 'Accuracy (%)', 'Rise Probability (%)', 'Last Actual Price',
            'Predicted Future Price', 'Recommendation', 'Analysis'
        ]
        result_df = filtered_df[result_columns]

        recommendations = result_df.to_dict(orient='records')
        return {
            "message": f"{len(recommendations)}개의 추천 주식을 찾았습니다",
            "recommendations": recommendations
        }

    def get_recommendations_with_sentiment(self):
        """
        get_stock_recommendations에서 가져온 추천 주식 중 
        ticker_sentiment_analysis 테이블에서 average_sentiment_score >= 0.15인 주식만 필터링하고,
        두 데이터 소스의 정보를 결합하여 반환합니다.
        """
        stock_recs = self.get_stock_recommendations()
        recommendations = stock_recs.get("recommendations", [])
        if not recommendations:
            return {"message": "추천 주식이 없습니다", "results": []}

        sentiment_response = supabase.table("ticker_sentiment_analysis").select("*").gte("average_sentiment_score", 0.15).execute()
        if not sentiment_response.data:
            return {"message": "감정 분석 데이터가 없습니다", "results": []}

        ticker_to_recommendation = {
            STOCK_TO_TICKER.get(rec["Stock"]): rec 
            for rec in recommendations 
            if rec["Stock"] in STOCK_TO_TICKER
        }
        sentiment_data = {item["ticker"]: item for item in sentiment_response.data}

        results = []
        for ticker, sentiment in sentiment_data.items():
            if ticker in ticker_to_recommendation:
                recommendation = ticker_to_recommendation[ticker]
                combined_data = {
                    "ticker": ticker,
                    "stock_name": recommendation["Stock"],
                    "accuracy": recommendation["Accuracy (%)"],
                    "rise_probability": recommendation["Rise Probability (%)"],
                    "last_actual_price": recommendation["Last Actual Price"],
                    "predicted_future_price": recommendation["Predicted Future Price"],
                    "recommendation": recommendation["Recommendation"],
                    "analysis": recommendation["Analysis"],
                    "average_sentiment_score": sentiment["average_sentiment_score"],
                    "article_count": sentiment["article_count"],
                    "calculation_date": sentiment["calculation_date"]
                }
                results.append(combined_data)

        return {
            "message": f"{len(results)}개의 추천 주식을 분석했습니다",
            "results": results
        }

    def fetch_and_store_sentiment_for_recommendations(self):
        """
        추천 주식과 보유 중인 주식에 대해 뉴스 감정 데이터를 가져오고, Supabase에 저장하며,
        감정 분석과 추천 정보를 통합하여 반환합니다.
        """
        # 추천 주식 목록 가져오기
        stock_recs = self.get_stock_recommendations()
        recommendations = stock_recs.get("recommendations", [])
        
        # 추천 주식의 티커 목록 생성
        recommended_tickers = [STOCK_TO_TICKER.get(rec["Stock"]) for rec in recommendations if rec["Stock"] in STOCK_TO_TICKER]
        
        # 보유 주식 정보 가져오기
        balance_result = get_overseas_balance()
        holdings = []
        
        if balance_result.get("rt_cd") == "0" and "output1" in balance_result:
            holdings = balance_result.get("output1", [])
            print(f"보유 주식 정보를 성공적으로 가져왔습니다. 총 {len(holdings)}개 종목 보유 중")
        else:
            print(f"보유 주식 정보를 가져오는데 실패했습니다: {balance_result.get('msg1', '알 수 없는 오류')}")
        
        # 보유 주식의 티커 목록 생성
        holding_tickers = [item.get("ovrs_pdno") for item in holdings if item.get("ovrs_pdno")]
        
        # 추천 주식과 보유 주식의 티커를 합치고 중복 제거
        all_tickers = list(set(recommended_tickers + holding_tickers))
        
        if not all_tickers:
            return {"message": "분석할 티커가 없습니다", "results": []}

        print(f"분석할 티커 목록 ({len(all_tickers)}개): {all_tickers}")

        api_key = settings.ALPHA_VANTAGE_API_KEY
        relevance_threshold = 0.2
        sleep_interval = 5
        yesterday = (datetime.now() - timedelta(days=3)).strftime("%Y%m%dT0000")

        base_url = "https://www.alphavantage.co/query"
        params = {
            "function": "NEWS_SENTIMENT",
            "time_from": yesterday,
            "limit": 100,
            "apikey": api_key
        }

        ticker_to_stock = {ticker: stock for stock, ticker in STOCK_TO_TICKER.items()}
        recommendations_by_ticker = {
            STOCK_TO_TICKER[rec["Stock"]]: rec for rec in recommendations if rec["Stock"] in STOCK_TO_TICKER
        }
        
        # 보유 주식 정보를 ticker로 매핑
        holdings_by_ticker = {item.get("ovrs_pdno"): item for item in holdings if item.get("ovrs_pdno")}

        # 기존 감정 분석 데이터 삭제
        print("기존 감정 분석 데이터 삭제 중...")
        supabase.table("ticker_sentiment_analysis").delete().gte("ticker", "").execute()
        print("기존 감정 분석 데이터 삭제 완료")

        results = []
        for ticker in all_tickers:
            print(f"{ticker} 처리 중...")
            params["tickers"] = ticker

            response = requests.get(base_url, params=params)
            if response.status_code != 200:
                results.append({
                    "ticker": ticker,
                    "stock_name": ticker_to_stock.get(ticker, ticker),  # 티커명이 없으면 티커 자체를 표시
                    "message": "API 호출 실패",
                    "is_recommended": ticker in recommended_tickers,
                    "is_holding": ticker in holding_tickers,
                    "recommendation_info": recommendations_by_ticker.get(ticker, {}),
                    "holding_info": holdings_by_ticker.get(ticker, {})
                })
                time.sleep(sleep_interval)
                continue

            api_data = response.json()
            feed = api_data.get('feed', [])

            articles = [
                float(sentiment['ticker_sentiment_score'])
                for article in feed
                for sentiment in article.get('ticker_sentiment', [])
                if sentiment['ticker'] == ticker and float(sentiment['relevance_score']) >= relevance_threshold
            ]

            if not articles:
                results.append({
                    "ticker": ticker,
                    "stock_name": ticker_to_stock.get(ticker, ticker),
                    "message": "관련 기사 없음",
                    "is_recommended": ticker in recommended_tickers,
                    "is_holding": ticker in holding_tickers,
                    "recommendation_info": recommendations_by_ticker.get(ticker, {}),
                    "holding_info": holdings_by_ticker.get(ticker, {})
                })
                time.sleep(sleep_interval)
                continue

            average_sentiment = sum(articles) / len(articles)
            article_count = len(articles)
            calculation_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # 해당 티커에 대한 데이터 추가
            # 테이블 스키마에 맞게 필드 조정
            supabase_data = {
                "ticker": ticker,
                "average_sentiment_score": average_sentiment,
                "article_count": article_count,
                "calculation_date": calculation_date
            }
            supabase.table("ticker_sentiment_analysis").insert(supabase_data).execute()

            results.append({
                "ticker": ticker,
                "stock_name": ticker_to_stock.get(ticker, ticker),
                "average_sentiment_score": average_sentiment,
                "article_count": article_count,
                "is_recommended": ticker in recommended_tickers,
                "is_holding": ticker in holding_tickers,
                "recommendation_info": recommendations_by_ticker.get(ticker, {}),
                "holding_info": holdings_by_ticker.get(ticker, {})
            })
            time.sleep(sleep_interval)

        return {
            "message": f"{len(results)}개의 티커(추천 주식: {len(recommended_tickers)}개, 보유 주식: {len(holding_tickers)}개)를 분석했습니다",
            "results": results
        }

    def get_combined_recommendations_with_technical_and_sentiment(self):
        """
        추천 주식 목록을 기술적 지표(stock_recommendations 테이블)와 감정 분석(ticker_sentiment_analysis 테이블)을
        결합하여 반환합니다.
        - stock_recommendations에서 골든_크로스=true, MACD_매수_신호=true, RSI<50 중 하나 이상 만족하는 종목 필터링
        - ticker_sentiment_analysis에서 average_sentiment_score >= 0.15인 데이터와 결합
        - get_stock_recommendations의 결과와 통합하여 반환
        - 추가 조건: sentiment_score와 기술적 지표를 기반으로 매수 추천 필터링
        """
        try:
            # 1. 기술적 지표 데이터 조회
            tech_response = supabase.table("stock_recommendations").select("*").order("날짜", desc=True).execute()
            if not tech_response.data:
                return {"message": "기술적 지표 데이터가 없습니다", "results": []}
            
            tech_df = pd.DataFrame(tech_response.data)
            
            # 데이터 타입 변환
            tech_df["골든_크로스"] = tech_df["골든_크로스"].astype(bool)
            tech_df["MACD_매수_신호"] = tech_df["MACD_매수_신호"].astype(bool)
            tech_df["RSI"] = pd.to_numeric(tech_df["RSI"])
            
            # 필터링: 골든_크로스=true, MACD_매수_신호=true, RSI<50 중 하나 이상
            mask_golden = tech_df["골든_크로스"] == True
            mask_macd = tech_df["MACD_매수_신호"] == True
            mask_rsi = tech_df["RSI"] < 50
            combined_mask = np.logical_or.reduce([mask_golden, mask_macd, mask_rsi])
            filtered_tech_df = tech_df[combined_mask]
            
            if filtered_tech_df.empty:
                return {"message": "조건을 만족하는 기술적 지표가 없습니다", "results": []}
            
            # 2. 주가 예측 데이터 조회
            stock_recs = self.get_stock_recommendations()
            recommendations = stock_recs.get("recommendations", [])
            if not recommendations:
                return {"message": "추천 주식이 없습니다", "results": []}
            
            # 3. 감정 분석 데이터 조회
            sentiment_response = supabase.table("ticker_sentiment_analysis").select("*").gte("average_sentiment_score", 0.15).execute()
            
            # 4. 데이터 매핑 준비
            tech_map = {row["종목"]: row.to_dict() for _, row in filtered_tech_df.iterrows()}
            sentiment_map = {item["ticker"]: item for item in sentiment_response.data} if sentiment_response.data else {}
            
            # 5. 결과 통합
            results = []
            for rec in recommendations:
                stock_name = rec["Stock"]
                if stock_name not in STOCK_TO_TICKER:
                    continue
                
                ticker = STOCK_TO_TICKER[stock_name]
                tech_data = tech_map.get(stock_name)
                if tech_data is None:
                    continue  # 기술적 지표가 없으면 제외
                
                sentiment = sentiment_map.get(ticker)
                
                # 통합 데이터 생성
                combined_data = {
                    "ticker": ticker,
                    "stock_name": stock_name,
                    "accuracy": rec["Accuracy (%)"],
                    "rise_probability": rec["Rise Probability (%)"],
                    "last_price": rec["Last Actual Price"],
                    "predicted_price": rec["Predicted Future Price"],
                    "recommendation": rec["Recommendation"],
                    "analysis": rec["Analysis"],
                    "sentiment_score": sentiment["average_sentiment_score"] if sentiment else None,
                    "article_count": sentiment["article_count"] if sentiment else None,
                    "sentiment_date": sentiment["calculation_date"] if sentiment else None,
                    "technical_date": tech_data["날짜"],
                    "sma20": float(tech_data["SMA20"]),
                    "sma50": float(tech_data["SMA50"]),
                    "golden_cross": bool(tech_data["골든_크로스"]),
                    "rsi": float(tech_data["RSI"]),
                    "macd": float(tech_data["MACD"]),
                    "signal": float(tech_data["Signal"]),
                    "macd_buy_signal": bool(tech_data["MACD_매수_신호"]),
                    "technical_recommended": bool(tech_data["추천_여부"])
                }
                results.append(combined_data)
            
            # 6. 매수 추천 조건에 따른 추가 필터링 후 순위 계산
            final_results = []
            for item in results:
                sentiment_score = item["sentiment_score"]
                tech_conditions = [item["golden_cross"], item["rsi"] < 50, item["macd_buy_signal"]]
                
                if sentiment_score is not None and sentiment_score >= 0.15:
                    if sum(tech_conditions) >= 2:
                        final_results.append(item)
                else:
                    if sum(tech_conditions) >= 3:
                        final_results.append(item)

            # 7. 종합 점수 계산 및 정렬
            for item in final_results:
                sentiment_score = item["sentiment_score"] if item["sentiment_score"] is not None else 0.0
                tech_conditions_count = (
                    1.5 * item["golden_cross"] +
                    1.0 * (item["rsi"] < 50) +
                    1.0 * item["macd_buy_signal"]
                )
                item["composite_score"] = (
                    0.3 * item["rise_probability"] +
                    0.4 * tech_conditions_count +
                    0.3 * sentiment_score
                )

            final_results.sort(key=lambda x: x["composite_score"], reverse=True)

            # 8. 결과 반환
            return {
                "message": f"{len(final_results)}개의 매수 추천 주식을 찾았습니다",
                "results": final_results
            }
        
        except Exception as e:
            print(f"오류 발생: {str(e)}")
            import traceback
            print(traceback.format_exc())  # 상세 스택 트레이스 출력
            raise Exception(f"추천 주식 분석 중 오류: {str(e)}")

    def get_stocks_to_sell(self):
        """
        매도 대상 종목을 식별하는 함수
        
        매도 조건:
        1. 구매가 대비 현재가가 +5% 이상(익절) 또는 -7% 이하(손절)인 종목
        2. 감성 점수 < -0.15이고 기술적 지표 중 2개 이상 매도 신호인 종목
        3. 기술적 지표 중 3개 이상 매도 신호인 종목
        
        반환값:
        - sell_candidates: 매도 대상 종목 목록
        - technical_data: 종목별 기술적 지표 데이터
        - sentiment_data: 종목별 감성 분석 데이터
        """
        try:
            # 1. 보유 종목 정보 가져오기
            balance_result = get_overseas_balance()
            if balance_result.get("rt_cd") != "0" or "output1" not in balance_result:
                return {
                    "message": f"보유 종목 정보를 가져오는데 실패했습니다: {balance_result.get('msg1', '알 수 없는 오류')}",
                    "sell_candidates": []
                }
            
            holdings = balance_result.get("output1", [])
            if not holdings:
                return {
                    "message": "보유 종목이 없습니다",
                    "sell_candidates": []
                }
            
            print(f"보유 종목 정보를 성공적으로 가져왔습니다. 총 {len(holdings)}개 종목 보유 중")
            
            # 2. 티커와 한글명 매핑 생성
            ticker_to_korean = {}
            korean_to_ticker = {}
            
            for item in holdings:
                ticker = item.get("ovrs_pdno")
                name = item.get("ovrs_item_name")
                if ticker and name:
                    ticker_to_korean[ticker] = name
                    korean_to_ticker[name] = ticker
            
            # 3. 기술적 지표 데이터 가져오기
            tech_response = supabase.table("stock_recommendations").select("*").order("날짜", desc=True).execute()
            tech_data = pd.DataFrame(tech_response.data) if tech_response.data else pd.DataFrame()
            
            if not tech_data.empty:
                # 데이터 타입 변환
                tech_data["골든_크로스"] = tech_data["골든_크로스"].astype(bool)
                tech_data["MACD_매수_신호"] = tech_data["MACD_매수_신호"].astype(bool)
                tech_data["RSI"] = pd.to_numeric(tech_data["RSI"])
                
                # 최신 데이터만 필터링 (종목별 가장 최근 날짜의 데이터)
                tech_data = tech_data.sort_values("날짜", ascending=False)
                tech_data = tech_data.drop_duplicates(subset=["종목"], keep="first")
            
            # 4. 감성 분석 데이터 가져오기
            sentiment_response = supabase.table("ticker_sentiment_analysis").select("*").execute()
            sentiment_data = {item["ticker"]: item for item in sentiment_response.data} if sentiment_response.data else {}
            
            # 5. 매도 대상 종목 식별
            sell_candidates = []
            
            for item in holdings:
                ticker = item.get("ovrs_pdno")
                stock_name = item.get("ovrs_item_name")
                purchase_price = float(item.get("pchs_avg_pric", 0))
                current_price = float(item.get("now_pric2", 0))
                quantity = int(item.get("ovrs_cblc_qty", 0))
                exchange_code = item.get("ovrs_excg_cd", "")
                
                # 가격 변동률 계산
                price_change_percent = ((current_price - purchase_price) / purchase_price) * 100 if purchase_price > 0 else 0
                
                # 매도 근거와 신호 수를 추적할 변수들
                sell_reasons = []
                technical_sell_signals = 0
                
                # 조건 1: 가격 기반 매도 (익절/손절)
                if price_change_percent >= 5:
                    sell_reasons.append(f"익절 조건 충족: 구매가 대비 {price_change_percent:.2f}% 상승")
                elif price_change_percent <= -7:
                    sell_reasons.append(f"손절 조건 충족: 구매가 대비 {price_change_percent:.2f}% 하락")
                
                # 기술적 지표 확인
                tech_record = None
                if not tech_data.empty:
                    tech_filtered = tech_data[tech_data["종목"] == stock_name]
                    if not tech_filtered.empty:
                        tech_record = tech_filtered.iloc[0].to_dict()
                
                tech_sell_signals_details = []
                if tech_record:
                    # 기술적 지표 매도 신호 확인
                    if not tech_record["골든_크로스"]:  # 데드 크로스는 매도 신호
                        technical_sell_signals += 1
                        tech_sell_signals_details.append("데드 크로스")
                    
                    if tech_record["RSI"] > 70:  # RSI 70 이상은 과매수 구간(매도 신호)
                        technical_sell_signals += 1
                        tech_sell_signals_details.append(f"RSI 과매수({tech_record['RSI']:.2f})")
                    
                    if not tech_record["MACD_매수_신호"]:  # MACD 매수 신호가 없으면 매도 신호
                        technical_sell_signals += 1
                        tech_sell_signals_details.append("MACD 매도 신호")
                
                # 감성 분석 데이터 확인
                sentiment_score = None
                if ticker in sentiment_data:
                    sentiment_score = sentiment_data[ticker].get("average_sentiment_score")
                
                # 조건 3: 기술적 지표 중 3개 이상 매도 신호 (가장 강력한 매도 신호부터 체크)
                if technical_sell_signals >= 3:
                    sell_reasons.append(f"모든 기술적 지표가 매도 신호: {', '.join(tech_sell_signals_details)}")
                # 조건 2: 감성 점수 < -0.15이고 기술적 지표 중 2개 이상 매도 신호
                elif sentiment_score is not None and sentiment_score < -0.15 and technical_sell_signals >= 2:
                    sell_reasons.append(f"부정적 감성({sentiment_score:.2f})과 기술적 매도 신호({technical_sell_signals}개): {', '.join(tech_sell_signals_details)}")
                
                # 매도 대상 판단
                if sell_reasons:
                    sell_candidates.append({
                        "ticker": ticker,
                        "stock_name": stock_name,
                        "purchase_price": purchase_price,
                        "current_price": current_price,
                        "price_change_percent": price_change_percent,
                        "quantity": quantity,
                        "exchange_code": exchange_code,
                        "sell_reasons": sell_reasons,
                        "technical_sell_signals": technical_sell_signals,
                        "technical_sell_details": tech_sell_signals_details if tech_sell_signals_details else None,
                        "sentiment_score": sentiment_score,
                        "technical_data": tech_record
                    })
            
            # 가격 변동률이 큰 순서로 정렬 (절대값 기준)
            sell_candidates.sort(key=lambda x: abs(x["price_change_percent"]), reverse=True)
            
            return {
                "message": f"{len(sell_candidates)}개의 매도 대상 종목을 식별했습니다",
                "sell_candidates": sell_candidates
            }
            
        except Exception as e:
            print(f"매도 대상 종목 식별 중 오류 발생: {str(e)}")
            import traceback
            print(traceback.format_exc())
            return {
                "message": f"매도 대상 종목 식별 중 오류 발생: {str(e)}",
                "sell_candidates": []
            }