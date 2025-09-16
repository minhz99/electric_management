# main.py
"""
File chính để chạy hệ thống quản lý điện năng
"""

import logging
import sys
from processor import ElectricityProcessor

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('electricity_management.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

def main():
    """Hàm main chạy ứng dụng"""
    try:
        logger.info("=== Khởi động hệ thống quản lý điện năng ===")
        
        # Khởi tạo và chạy processor
        processor = ElectricityProcessor()
        processor.run()
        
    except KeyboardInterrupt:
        logger.info("Hệ thống đã được dừng bởi người dùng")
    except Exception as e:
        logger.error(f"Lỗi nghiêm trọng: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
