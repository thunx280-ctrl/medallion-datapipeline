import pandas as pd
import os

# def read_data(file_name, buffer):
#     if file_name.endswith(".csv"):
#         data = pd.read_csv(buffer)
#     elif file_name.endwith(".xlsx"):
#         data = pd.read_excel(buffer, engine='openpyxl')
#     return data

def read_data(file_name, buffer, sheet_name=0):
    """
    Ép dtype=str cho mọi cột (nguyên tắc Bronze: tất cả cột
    đều là TEXT), giữ nguyên NaN thật (không biến thành chuỗi "nan").

    sheet_name=0    -> đọc sheet đầu tiên (mặc định, dùng cho hầu hết nguồn)
    sheet_name=None -> đọc TẤT CẢ sheet, trả về dict {sheet_name: DataFrame}
                       (dùng cho sales_target_plan - nhiều version/sheet)
    """
    file_name_lower = file_name.lower()

    if file_name_lower.endswith(".csv"):
        # encoding='utf-8-sig' để loại bỏ BOM (\ufeff) ở đầu file CSV tiếng Việt
        # keep_default_na=False: giữ ô trống là "" thay vì tự suy luận thành NaN
        data = pd.read_csv(
        buffer,
        encoding="utf-8-sig",
        dtype=str,
        keep_default_na=False,
        )

    elif file_name_lower.endswith((".xlsx", ".xlsm")):
        data = pd.read_excel(
        buffer,
        sheet_name=sheet_name,
        engine="openpyxl",
        dtype=str,
        keep_default_na=False,
        )

    elif file_name_lower.endswith(".xlsb"):
        data = pd.read_excel(
        buffer,
        sheet_name=sheet_name,
        engine="pyxlsb",
        dtype=str,
        keep_default_na=False,
        )
    else:
        raise ValueError(
        f"Định dạng file '{file_name}' chưa được hỗ trợ. "
        "Chỉ hỗ trợ: .csv, .xlsx, .xlsm, .xlsb"
        )
    return data


# if __name__ == "__main__":
#     # Test nhanh: đọc thử 1 file CSV mẫu (chỉnh path trước khi chạy)
#     with open("VietDist_SampleData/SRC03_customer_master.csv", "rb") as f:
#         df = read_data("SRC03_customer_master.csv", f)
#     print(df.shape)
#     print(df.columns.tolist())  # kiểm tra cột đầu tiên KHÔNG còn dính \ufeff
#     print(df.head())
