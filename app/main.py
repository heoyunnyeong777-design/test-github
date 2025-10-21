from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
from app.api.api import api_router
from app.services.economic_service import update_economic_data_in_background
from app.utils.scheduler import (
    start_scheduler, stop_scheduler, 
    start_sell_scheduler, stop_sell_scheduler,
    start_economic_data_scheduler, stop_economic_data_scheduler
)
from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: runs once when app starts
    await startup()
    yield
    # Shutdown: 필요한 정리 작업
    stop_scheduler()  # 매수 스케줄러 종료
    stop_sell_scheduler()  # 매도 스케줄러 종료
    stop_economic_data_scheduler()  # 경제 데이터 스케줄러 종료

app = FastAPI(title="주식 분석 및 추천 API", lifespan=lifespan)

# CORS 미들웨어 설정
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 모든 오리진 허용 (프로덕션에서는 특정 도메인으로 제한 권장)
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# API 라우터 등록 (중앙 관리 방식)
app.include_router(api_router)

@app.get("/")
def read_root():
    return {"message": "주식 분석 및 추천 API에 오신 것을 환영합니다"}

# APScheduler 대신 직접 실행
async def startup():
    # 시작 시 즉시 한 번 경제 데이터 수집 실행
    print("서비스 시작 시 경제 데이터 수집을 즉시 실행합니다...")
    await update_economic_data_in_background()
    print("초기 경제 데이터 수집이 완료되었습니다.")
    
    # 경제 데이터 업데이트 스케줄러 시작 (매일 한국시간 새벽 6시 5분에 실행)
    start_economic_data_scheduler()
    # 주식 자동매매 스케줄러 시작
    start_scheduler()
    start_sell_scheduler()
    print("경제 데이터 업데이트 스케줄러가 시작되었습니다. (매일 한국시간 새벽 6시 5분)")
    print("주식 자동매매 스케줄러가 시작되었습니다.")
    print("주식 자동매도 스케줄러가 시작되었습니다.")

if __name__ == "__main__":
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)