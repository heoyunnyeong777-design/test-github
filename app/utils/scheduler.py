import asyncio
import schedule
import time
import pytz
from datetime import datetime, timedelta
import threading
from app.services.stock_recommendation_service import StockRecommendationService
from app.services.balance_service import get_current_price, order_overseas_stock, get_all_overseas_balances
from app.core.config import settings
import logging
from app.services.economic_service import update_economic_data_in_background

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('stock_scheduler.log')
    ]
)
logger = logging.getLogger('stock_scheduler')

class StockScheduler:
    """주식 자동매매 스케줄러 클래스"""
    
    def __init__(self):
        self.recommendation_service = StockRecommendationService()
        self.running = False
        self.sell_running = False  # 매도 스케줄러 실행 상태
        self.scheduler_thread = None
    
    def start(self):
        """매수 스케줄러 시작"""
        if self.running:
            logger.warning("매수 스케줄러가 이미 실행 중입니다.")
            return False
        
        # 한국 시간 기준 밤 12시(00:00)에 매수 작업 실행
        schedule.every().day.at("00:00").do(self._run_auto_buy)
        
        # 별도 스레드에서 스케줄러 실행
        self.running = True
        self.scheduler_thread = threading.Thread(target=self._run_scheduler)
        self.scheduler_thread.daemon = True
        self.scheduler_thread.start()
        
        logger.info("주식 자동매매 스케줄러가 시작되었습니다. 한국 시간 밤 12시(00:00)에 매수 작업이 실행됩니다.")
        return True
    
    def stop(self):
        """매수 스케줄러 중지"""
        if not self.running:
            logger.warning("매수 스케줄러가 실행 중이 아닙니다.")
            return False
        
        self.running = False
        if self.scheduler_thread:
            self.scheduler_thread.join(timeout=5)
        
        # 매수 관련 작업 취소 (sell 스케줄러는 유지)
        buy_jobs = [job for job in schedule.jobs if job.job_func.__name__ == '_run_auto_buy']
        for job in buy_jobs:
            schedule.cancel_job(job)
        
        logger.info("매수 스케줄러가 중지되었습니다.")
        return True
    
    def start_sell_scheduler(self):
        """매도 스케줄러 시작"""
        if self.sell_running:
            logger.warning("매도 스케줄러가 이미 실행 중입니다.")
            return False
        
        # 1분마다 매도 작업 실행
        schedule.every(1).minutes.do(self._run_auto_sell)
        
        # 스케줄러 스레드가 없으면 시작
        if not self.running and not self.scheduler_thread:
            self.scheduler_thread = threading.Thread(target=self._run_scheduler)
            self.scheduler_thread.daemon = True
            self.scheduler_thread.start()
        
        self.sell_running = True
        logger.info("매도 스케줄러가 시작되었습니다. 1분마다 매도 대상을 확인합니다.")
        return True
    
    def stop_sell_scheduler(self):
        """매도 스케줄러 중지"""
        if not self.sell_running:
            logger.warning("매도 스케줄러가 실행 중이 아닙니다.")
            return False
        
        # 매도 관련 작업만 취소
        sell_jobs = [job for job in schedule.jobs if job.job_func.__name__ == '_run_auto_sell']
        for job in sell_jobs:
            schedule.cancel_job(job)
        
        self.sell_running = False
        
        # 매수, 매도 모두 중지된 경우 스레드 종료
        if not self.running and self.scheduler_thread:
            self.scheduler_thread.join(timeout=5)
            self.scheduler_thread = None
            
        logger.info("매도 스케줄러가 중지되었습니다.")
        return True
    
    def _run_scheduler(self):
        """스케줄러 백그라운드 실행 함수"""
        while self.running or self.sell_running:
            schedule.run_pending()
            time.sleep(1)
    
    def _run_auto_buy(self):
        """자동 매수 실행 함수 - 스케줄링된 시간에 실행됨"""
        try:
            logger.info("자동 매수 작업 시작")
            asyncio.run(self._execute_auto_buy())
            logger.info("자동 매수 작업 완료")
            return True
        except Exception as e:
            logger.error(f"자동 매수 작업 중 오류 발생: {str(e)}", exc_info=True)
            return False
    
    def _run_auto_sell(self):
        """자동 매도 실행 함수 - 1분마다 실행됨"""
        try:
            logger.info("자동 매도 작업 시작")
            asyncio.run(self._execute_auto_sell())
            logger.info("자동 매도 작업 완료")
            return True
        except Exception as e:
            logger.error(f"자동 매도 작업 중 오류 발생: {str(e)}", exc_info=True)
            return False
    
    async def _execute_auto_sell(self):
        """자동 매도 실행 로직"""
        # 현재 시간이 미국 장 시간인지 확인 (서머타임 고려)
        now_in_korea = datetime.now(pytz.timezone('Asia/Seoul'))
        
        # 미국 뉴욕 시간 (서머타임 자동 고려)
        now_in_ny = datetime.now(pytz.timezone('America/New_York'))
        ny_hour = now_in_ny.hour
        ny_minute = now_in_ny.minute
        ny_weekday = now_in_ny.weekday()  # 0=월요일, 6=일요일
        
        # 미국 주식 시장은 평일(월-금) 9:30 AM - 4:00 PM ET
        is_weekday = 0 <= ny_weekday <= 4  # 월요일에서 금요일까지
        is_market_open_time = (
            (ny_hour == 9 and ny_minute >= 30) or
            (10 <= ny_hour < 16) or
            (ny_hour == 16 and ny_minute == 0)
        )
        
        is_market_hours = is_weekday and is_market_open_time
        
        if not is_market_hours:
            # 주말이거나 장 시간이 아닌 경우
            logger.info(f"현재 시간 {now_in_korea.strftime('%Y-%m-%d %H:%M:%S')} (뉴욕: {now_in_ny.strftime('%Y-%m-%d %H:%M:%S')})은 미국 장 시간이 아닙니다. 매도 작업을 건너뜁니다.")
            return
        
        logger.info(f"미국 장 시간 확인: {now_in_korea.strftime('%Y-%m-%d %H:%M:%S')} (뉴욕: {now_in_ny.strftime('%Y-%m-%d %H:%M:%S')})")
        
        # 매도 대상 종목 조회
        sell_candidates_result = self.recommendation_service.get_stocks_to_sell()
        
        if not sell_candidates_result or not sell_candidates_result.get("sell_candidates"):
            logger.info("매도 대상 종목이 없습니다.")
            return
        
        sell_candidates = sell_candidates_result.get("sell_candidates", [])
        logger.info(f"매도 대상 종목 {len(sell_candidates)}개를 찾았습니다.")
        
        # 각 종목에 대해 매도 주문 실행
        for candidate in sell_candidates:
            try:
                ticker = candidate["ticker"]
                stock_name = candidate["stock_name"]
                exchange_code = candidate["exchange_code"]
                quantity = candidate["quantity"]
                
                # 매도 근거 로그 출력
                sell_reasons = candidate.get("sell_reasons", [])
                reasons_str = "; ".join(sell_reasons)
                logger.info(f"{stock_name}({ticker}) 매도 근거: {reasons_str}")
                
                # 거래소 코드 변환 (API 요청에 맞게 변환)
                api_exchange_code = exchange_code
                if exchange_code == "NASD":
                    api_exchange_code = "NAS"
                elif exchange_code == "NYSE":
                    api_exchange_code = "NYS"
                
                # 현재가 조회
                price_params = {
                    "AUTH": "",
                    "EXCD": api_exchange_code,  # 변환된 거래소 코드 사용
                    "SYMB": ticker
                }
                
                logger.info(f"{stock_name}({ticker}) 현재가 조회 요청. 거래소: {api_exchange_code}, 심볼: {ticker}")
                price_result = get_current_price(price_params)
                
                if price_result.get("rt_cd") != "0":
                    logger.error(f"{stock_name}({ticker}) 현재가 조회 실패: {price_result.get('msg1', '알 수 없는 오류')}")
                    # API 속도 제한에 도달했을 때 더 오래 대기
                    if "초당" in price_result.get('msg1', ''):
                        await asyncio.sleep(3)  # 속도 제한 오류 시 3초 대기
                    continue
                
                # 현재가 추출 (안전하게 처리)
                last_price = price_result.get("output", {}).get("last", "")
                try:
                    # 빈 문자열이나 None 체크
                    if not last_price or last_price == "":
                        logger.error(f"{stock_name}({ticker}) 현재가가 비어있습니다. 다음 API 호출에서 다시 시도합니다.")
                        await asyncio.sleep(2)  # 잠시 기다렸다가 넘어감
                        continue
                    
                    current_price = float(last_price)
                    
                    if current_price <= 0:
                        logger.error(f"{stock_name}({ticker}) 현재가가 유효하지 않습니다: {current_price}")
                        continue
                except ValueError as ve:
                    logger.error(f"{stock_name}({ticker}) 현재가 변환 오류: {str(ve)}, 값: '{last_price}'")
                    continue
                
                # 매도 주문 실행
                order_data = {
                    "CANO": settings.KIS_CANO,
                    "ACNT_PRDT_CD": settings.KIS_ACNT_PRDT_CD,
                    "OVRS_EXCG_CD": exchange_code,  # API 문서에 따라 원래대로 exchange_code 사용
                    "PDNO": ticker,
                    "ORD_DVSN": "00",  # 지정가
                    "ORD_QTY": str(quantity),
                    "OVRS_ORD_UNPR": str(current_price),
                    "is_buy": False  # 매도
                }
                
                logger.info(f"{stock_name}({ticker}) 매도 주문 실행: 수량 {quantity}주, 가격 ${current_price}")
                order_result = order_overseas_stock(order_data)
                
                if order_result.get("rt_cd") == "0":
                    logger.info(f"{stock_name}({ticker}) 매도 주문 성공: {order_result.get('msg1', '주문이 접수되었습니다.')}")
                else:
                    logger.error(f"{stock_name}({ticker}) 매도 주문 실패: {order_result.get('msg1', '알 수 없는 오류')}")
                
                # 요청 간 지연 (API 요청 제한 방지)
                await asyncio.sleep(2)  # 1초에서 2초로 증가
                
            except Exception as e:
                logger.error(f"{candidate['stock_name']}({candidate['ticker']}) 매도 처리 중 오류: {str(e)}", exc_info=True)
                await asyncio.sleep(1)  # 오류 발생 시에도 잠시 대기
        
        logger.info("자동 매도 처리가 완료되었습니다.")
    
    async def _execute_auto_buy(self):
        """자동 매수 실행 로직"""
        # 보유 종목 조회
        try:
            balance_result = get_all_overseas_balances()
            if balance_result.get("rt_cd") != "0":
                logger.error(f"보유 종목 조회 실패: {balance_result.get('msg1', '알 수 없는 오류')}")
                return
            
            # 보유 종목 티커 추출
            holdings = balance_result.get("output1", [])
            holding_tickers = set()
            
            for item in holdings:
                ticker = item.get("ovrs_pdno")
                if ticker:
                    holding_tickers.add(ticker)
            
            logger.info(f"현재 보유 중인 종목 수: {len(holding_tickers)}")
        except Exception as e:
            logger.error(f"보유 종목 조회 중 오류 발생: {str(e)}", exc_info=True)
            return
            
        # StockRecommendationService에서 이미 필터링된 매수 대상 종목 가져오기
        recommendations = self.recommendation_service.get_combined_recommendations_with_technical_and_sentiment()
        
        if not recommendations or not recommendations.get("results"):
            logger.info("매수 대상 종목이 없습니다.")
            return
        
        buy_candidates = recommendations.get("results", [])
        
        if not buy_candidates:
            logger.info("매수 조건을 만족하는 종목이 없습니다.")
            return
        
        logger.info(f"매수 대상 종목 {len(buy_candidates)}개를 찾았습니다.")
        
        # 각 종목에 대해 API 호출하여 현재 체결가 조회 및 매수 주문
        for candidate in buy_candidates:
            try:
                ticker = candidate["ticker"]
                stock_name = candidate["stock_name"]
                
                # 거래소 코드 결정 (미국 주식 기준)
                if ticker.endswith(".X") or ticker.endswith(".N"):
                    # 거래소 구분이 티커에 포함된 경우
                    exchange_code = "NYSE" if ticker.endswith(".N") else "NASD"
                    pure_ticker = ticker.split(".")[0]
                else:
                    # 기본값 NASDAQ으로 설정
                    exchange_code = "NASD"
                    pure_ticker = ticker
                
                # 이미 보유 중인 종목인지 확인
                if pure_ticker in holding_tickers:
                    logger.info(f"{stock_name}({ticker}) - 이미 보유 중인 종목이므로 매수하지 않습니다.")
                    continue
                
                # 거래소 코드 변환 (API 요청에 맞게 변환)
                api_exchange_code = "NAS"
                if exchange_code == "NYSE":
                    api_exchange_code = "NYS"
                
                # 현재가 조회
                price_params = {
                    "AUTH": "",
                    "EXCD": api_exchange_code,  # 변환된 거래소 코드 사용
                    "SYMB": pure_ticker
                }
                
                logger.info(f"{stock_name}({ticker}) 현재가 조회 요청. 거래소: {api_exchange_code}, 심볼: {pure_ticker}")
                price_result = get_current_price(price_params)
                
                if price_result.get("rt_cd") != "0":
                    logger.error(f"{stock_name}({ticker}) 현재가 조회 실패: {price_result.get('msg1', '알 수 없는 오류')}")
                    continue
                
                # 현재가 추출
                current_price = float(price_result.get("output", {}).get("last", 0))
                
                if current_price <= 0:
                    logger.error(f"{stock_name}({ticker}) 현재가가 유효하지 않습니다: {current_price}")
                    continue
                
                # 매수 수량 계산 (종목당 계좌 잔고의 5%를 사용)
                # 실제 환경에서는 계좌 잔고를 조회하는 로직을 추가해야 함
                quantity = 1  # 기본값 설정
                
                # 매수 주문 실행
                order_data = {
                    "CANO": settings.KIS_CANO,
                    "ACNT_PRDT_CD": settings.KIS_ACNT_PRDT_CD,
                    "OVRS_EXCG_CD": exchange_code,  # API 문서에 따라 원래대로 exchange_code 사용
                    "PDNO": pure_ticker,
                    "ORD_DVSN": "00",  # 지정가
                    "ORD_QTY": str(quantity),
                    "OVRS_ORD_UNPR": str(current_price),
                    "is_buy": True
                }
                
                logger.info(f"{stock_name}({ticker}) 매수 주문 실행: 수량 {quantity}주, 가격 ${current_price}")
                order_result = order_overseas_stock(order_data)
                
                if order_result.get("rt_cd") == "0":
                    logger.info(f"{stock_name}({ticker}) 매수 주문 성공: {order_result.get('msg1', '주문이 접수되었습니다.')}")
                else:
                    logger.error(f"{stock_name}({ticker}) 매수 주문 실패: {order_result.get('msg1', '알 수 없는 오류')}")
                
                # 요청 간 지연 (API 요청 제한 방지)
                await asyncio.sleep(1)
                
            except Exception as e:
                logger.error(f"{candidate['stock_name']}({candidate['ticker']}) 매수 처리 중 오류: {str(e)}", exc_info=True)
        
        logger.info("자동 매수 처리가 완료되었습니다.")

# 싱글톤 인스턴스 생성
stock_scheduler = StockScheduler()

def start_scheduler():
    """매수 스케줄러 시작 함수"""
    return stock_scheduler.start()

def stop_scheduler():
    """매수 스케줄러 중지 함수"""
    return stock_scheduler.stop()

def start_sell_scheduler():
    """매도 스케줄러 시작 함수"""
    return stock_scheduler.start_sell_scheduler()

def stop_sell_scheduler():
    """매도 스케줄러 중지 함수"""
    return stock_scheduler.stop_sell_scheduler()

def get_scheduler_status():
    """스케줄러 상태 확인"""
    return {
        "buy_running": stock_scheduler.running,
        "sell_running": stock_scheduler.sell_running
    }

def run_auto_buy_now():
    """즉시 매수 실행 함수 (테스트용)"""
    stock_scheduler._run_auto_buy()
    
def run_auto_sell_now():
    """즉시 매도 실행 함수 (테스트용)"""
    stock_scheduler._run_auto_sell()

# 경제 데이터 스케줄러 관련 변수 및 함수
economic_data_scheduler_running = False
economic_data_scheduler_thread = None

def _run_economic_data_update():
    """경제 데이터 업데이트 실행 함수"""
    try:
        logger = logging.getLogger('economic_scheduler')
        logger.info("경제 데이터 업데이트 작업 시작")
        asyncio.run(update_economic_data_in_background())
        logger.info("경제 데이터 업데이트 작업 완료")
        return True
    except Exception as e:
        logger = logging.getLogger('economic_scheduler')
        logger.error(f"경제 데이터 업데이트 작업 중 오류 발생: {str(e)}", exc_info=True)
        return False

def _run_economic_scheduler():
    """경제 데이터 스케줄러 백그라운드 실행 함수"""
    global economic_data_scheduler_running
    while economic_data_scheduler_running:
        schedule.run_pending()
        time.sleep(1)

def start_economic_data_scheduler():
    """경제 데이터 업데이트 스케줄러 시작 함수"""
    global economic_data_scheduler_running, economic_data_scheduler_thread
    
    if economic_data_scheduler_running:
        logger = logging.getLogger('economic_scheduler')
        logger.warning("경제 데이터 스케줄러가 이미 실행 중입니다.")
        return False
    
    # 한국 시간 기준 새벽 6시 5분에 경제 데이터 업데이트 작업 실행
    schedule.every().day.at("06:05").do(_run_economic_data_update)
    
    # 별도 스레드에서 스케줄러 실행
    economic_data_scheduler_running = True
    economic_data_scheduler_thread = threading.Thread(target=_run_economic_scheduler)
    economic_data_scheduler_thread.daemon = True
    economic_data_scheduler_thread.start()
    
    logger = logging.getLogger('economic_scheduler')
    logger.info("경제 데이터 업데이트 스케줄러가 시작되었습니다. 한국 시간 새벽 6시 5분에 실행됩니다.")
    return True

def stop_economic_data_scheduler():
    """경제 데이터 업데이트 스케줄러 중지 함수"""
    global economic_data_scheduler_running, economic_data_scheduler_thread
    
    if not economic_data_scheduler_running:
        logger = logging.getLogger('economic_scheduler')
        logger.warning("경제 데이터 스케줄러가 실행 중이 아닙니다.")
        return False
    
    # 경제 데이터 관련 작업 취소
    economic_jobs = [job for job in schedule.jobs if job.job_func.__name__ == '_run_economic_data_update']
    for job in economic_jobs:
        schedule.cancel_job(job)
    
    economic_data_scheduler_running = False
    if economic_data_scheduler_thread:
        economic_data_scheduler_thread.join(timeout=5)
        economic_data_scheduler_thread = None
    
    logger = logging.getLogger('economic_scheduler')
    logger.info("경제 데이터 업데이트 스케줄러가 중지되었습니다.")
    return True

def run_economic_data_update_now():
    """즉시 경제 데이터 업데이트 실행 함수 (테스트용)"""
    return _run_economic_data_update() 