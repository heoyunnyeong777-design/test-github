from fastapi import APIRouter, BackgroundTasks, HTTPException, Depends
from app.schemas.stock import UpdateResponse
from app.utils.scheduler import run_economic_data_update_now
from datetime import date, datetime

router = APIRouter()

@router.post("/update", summary="경제 및 주식 데이터 업데이트", response_model=UpdateResponse)
async def update_economic_data(
    background_tasks: BackgroundTasks
):
    """
    경제 및 주식 데이터를 Supabase에 저장합니다.
    이 작업은 백그라운드에서 실행되어 API 응답을 블로킹하지 않습니다.
    
    DB에서 마지막 수집 날짜를 자동으로 찾아 그 다음 날부터 수집합니다.
    기존 데이터의 NULL 값은 새 데이터로 자동 업데이트됩니다.
    """
    try:
        # 백그라운드 작업으로 경제 데이터 업데이트 실행
        background_tasks.add_task(run_economic_data_update_now)
        
        return {
            "success": True,
            "message": "경제 데이터 업데이트가 백그라운드에서 시작되었습니다.",
            "total_records": 0,
            "updated_records": 0
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"데이터 업데이트 중 오류 발생: {str(e)}")