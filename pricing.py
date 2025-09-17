# pricing.py
"""
Module tính giá điện theo bậc thang + VAT
"""

from config import PRICE_TIERS, VAT_RATE


def calc_electricity_cost(kwh: float) -> dict:
    """
    Tính chi phí điện theo bậc thang và thuế VAT.
    
    Args:
        kwh (float): Điện năng tiêu thụ (kWh)

    Returns:
        dict: {
            "kWh": kwh,
            "subtotal": tiền trước thuế,
            "vat": thuế GTGT,
            "total": tổng tiền sau thuế,
            "detail": danh sách chi tiết từng bậc
        }
    """
    remaining = kwh
    subtotal = 0
    detail = []

    for tier_kwh, price in PRICE_TIERS:
        used = min(remaining, tier_kwh)
        cost = used * price
        if used > 0:
            detail.append({
                "used_kWh": used,
                "price": price,
                "cost": cost
            })
        subtotal += cost
        remaining -= used
        if remaining <= 0:
            break

    vat = subtotal * VAT_RATE
    total = subtotal + vat

    # Làm tròn tiền tệ về VNĐ (không có phần thập phân)
    subtotal = round(subtotal)
    vat = round(vat)
    total = round(total)

    return {
        "kWh": kwh,
        "subtotal": subtotal,
        "vat": vat,
        "total": total,
        "detail": detail
    }