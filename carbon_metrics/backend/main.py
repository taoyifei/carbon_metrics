"""
FastAPI 应用入口
"""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .config import get_app_config
from .routers import metrics_router, quality_router, equipment_router

# 创建应用
app = FastAPI(
    title="制冷系统指标平台",
    description="基于 cooling_system_v2 数据库的指标计算与展示平台",
    version="1.0.0",
)

# 配置 CORS
config = get_app_config()
app.add_middleware(
    CORSMiddleware,
    allow_origins=config.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 注册路由
app.include_router(metrics_router, prefix="/api")
app.include_router(quality_router, prefix="/api")
app.include_router(equipment_router, prefix="/api")


@app.get("/")
def root():
    """根路径"""
    return {"message": "制冷系统指标平台 API", "docs": "/docs"}


@app.get("/health")
def health():
    """健康检查"""
    return {"status": "ok"}
