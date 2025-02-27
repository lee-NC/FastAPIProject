class AccumulateCert:
    def __init__(self, locality_code="", total=0, total_pm=0, total_ps=0,
                 total_year=0, total_pm_year=0, total_ps_year=0,
                 total_month=0, total_pm_month=0, total_ps_month=0,
                 total_date=0, total_pm_date=0, total_ps_date=0,
                 total_extend=0, total_pm_extend=0, total_ps_extend=0,
                 total_year_extend=0, total_pm_year_extend=0, total_ps_year_extend=0,
                 total_month_extend=0, total_pm_month_extend=0, total_ps_month_extend=0,
                 total_date_extend=0, total_pm_date_extend=0, total_ps_date_extend=0,
                 total_year_expired=0, total_pm_year_expired=0, total_ps_year_expired=0,
                 total_month_expired=0, total_pm_month_expired=0, total_ps_month_expired=0,
                 total_date_expired=0, total_pm_date_expired=0, total_ps_date_expired=0,
                 total_vneid=0, total_vneid_date=0, total_online=0, total_online_date=0
                 ):
        self.locality_code = locality_code
        self.total = total
        self.total_pm = total_pm
        self.total_ps = total_ps
        self.total_year = total_year
        self.total_pm_year = total_pm_year
        self.total_ps_year = total_ps_year
        self.total_month = total_month
        self.total_pm_month = total_pm_month
        self.total_ps_month = total_ps_month
        self.total_date = total_date
        self.total_pm_date = total_pm_date
        self.total_ps_date = total_ps_date
        self.total_extend = total_extend
        self.total_pm_extend = total_pm_extend
        self.total_ps_extend = total_ps_extend
        self.total_year_extend = total_year_extend
        self.total_pm_year_extend = total_pm_year_extend
        self.total_ps_year_extend = total_ps_year_extend
        self.total_month_extend = total_month_extend
        self.total_pm_month_extend = total_pm_month_extend
        self.total_ps_month_extend = total_ps_month_extend
        self.total_date_extend = total_date_extend
        self.total_pm_date_extend = total_pm_date_extend
        self.total_ps_date_extend = total_ps_date_extend
        self.total_year_expired = total_year_expired
        self.total_pm_year_expired = total_pm_year_expired
        self.total_ps_year_expired = total_ps_year_expired
        self.total_month_expired = total_month_expired
        self.total_pm_month_expired = total_pm_month_expired
        self.total_ps_month_expired = total_ps_month_expired
        self.total_date_expired = total_date_expired
        self.total_pm_date_expired = total_pm_date_expired
        self.total_ps_date_expired = total_ps_date_expired
        self.total_vneid = total_vneid
        self.total_vneid_date = total_vneid_date
        self.total_online = total_online
        self.total_online_date = total_online_date

    def __repr__(self):  # Để hiển thị đối tượng dễ đọc hơn
        return (f"AccumulateCert(locality_code={self.locality_code}, "
                f"total={self.total}, total_pm={self.total_pm}, total_ps={self.total_ps}, "
                f"total_year={self.total_year}, total_pm_year={self.total_pm_year}, total_ps_year={self.total_ps_year}, "
                f"total_month={self.total_month}, total_pm_month={self.total_pm_month}, total_ps_month={self.total_ps_month}, "
                f"total_date={self.total_date}, total_pm_date={self.total_pm_date}, total_ps_date={self.total_ps_date}, "
                f"total_extend={self.total_extend}, total_pm_extend={self.total_pm_extend}, total_ps_extend={self.total_ps_extend}, "
                f"total_year_extend={self.total_year_extend}, total_pm_year_extend={self.total_pm_year_extend}, total_ps_year_extend={self.total_ps_year_extend},"
                f"total_month_extend={self.total_month_extend}, total_pm_month_extend={self.total_pm_month_extend}, total_ps_month_extend={self.total_ps_month_extend}, "
                f"total_date_extend={self.total_date_extend}, total_pm_date_extend={self.total_pm_date_extend}, total_ps_date_extend={self.total_ps_date_extend}, "
                f"total_year_expired={self.total_year_expired}, total_pm_year_expired={self.total_pm_year_expired}, total_ps_year_expired={self.total_ps_year_expired}, "
                f"total_month_expired={self.total_month_expired}, total_pm_month_expired={self.total_pm_month_expired}, total_ps_month_expired={self.total_ps_month_expired}, "
                f"total_date_expired={self.total_date_expired}, total_pm_date_expired={self.total_pm_date_expired}, total_ps_date_expired={self.total_ps_date_expired}, "
                f"total_vneid={self.total_vneid}, total_vneid_date={self.total_vneid_date}, total_online={self.total_online}, total_online_date={self.total_online_date})")


class CertOrderRegister:
    def __init__(self, ngay_lay_dl="", ma_tinh="", sdt_lh="", ten_kh="", so_gt="", ma_tb="", ma_don_hang="", dia_chi_ct="", ngay_tao_don="",
                 ngay_thuc_hien="", kenh_ban="", loai_yeu_cau="", ly_do_gd="", nguyen_nhan="", toc_do_id="",
                 ten_goi_cuoc="", gia_goi_cuoc="", trang_thai_tk=""
                 ):
        self.ngay_lay_dl = ngay_lay_dl
        self.ma_tinh = ma_tinh
        self.sdt_lh = sdt_lh
        self.ten_kh = ten_kh
        self.so_gt = so_gt
        self.ma_tb = ma_tb
        self.ma_don_hang = ma_don_hang
        self.dia_chi_ct = dia_chi_ct
        self.ngay_tao_don = ngay_tao_don
        self.ngay_thuc_hien = ngay_thuc_hien
        self.kenh_ban = kenh_ban
        self.loai_yeu_cau = loai_yeu_cau
        self.ly_do_gd = ly_do_gd
        self.nguyen_nhan = nguyen_nhan
        self.toc_do_id = toc_do_id
        self.ten_goi_cuoc = ten_goi_cuoc
        self.gia_goi_cuoc = gia_goi_cuoc
        self.trang_thai_tk = trang_thai_tk

    def __repr__(self):  # Để hiển thị đối tượng dễ đọc hơn
        return (f"CertOrderRegister(ngay_lay_dl={self.ngay_lay_dl}), ma_tinh={self.ma_tinh}, sdt_lh={self.sdt_lh}, ten_kh={self.ten_kh}, "
                f"so_gt={self.so_gt}, ma_tb={self.ma_tb}, ma_don_hang={self.ma_don_hang}, dia_chi_ct={self.dia_chi_ct}, "
                f"ngay_tao_don={self.ngay_tao_don}, ngay_thuc_hien={self.ngay_thuc_hien}, kenh_ban={self.kenh_ban}, "
                f"loai_yeu_cau={self.loai_yeu_cau}, ly_do_gd={self.ly_do_gd}, nguyen_nhan={self.nguyen_nhan}, toc_do_id={self.toc_do_id}, "
                f"ten_goi_cuoc={self.ten_goi_cuoc}, gia_goi_cuoc={self.gia_goi_cuoc}, trang_thai_tk={self.trang_thai_tk})")


class Locality:
    def __init__(self, area_local_code="", area_local_name="", province_id="", province_name="", province_code="", province_other_code="",
                 district_id="", district_name="", district_id_my_vnpt="", ward_id="", ward_name="", ward_id_my_vnpt="", co_tuyen_thu=False
                 ):
        self.area_local_code = area_local_code
        self.area_local_name = area_local_name
        self.province_id = province_id
        self.province_name = province_name
        self.province_code = province_code
        self.province_other_code = province_other_code
        self.district_id = district_id
        self.district_name = district_name
        self.district_id_my_vnpt = district_id_my_vnpt
        self.ward_id = ward_id
        self.ward_name = ward_name
        self.ward_id_my_vnpt = ward_id_my_vnpt
        self.co_tuyen_thu = co_tuyen_thu

    def __repr__(self):
        return (f"Locality(area_local_code={self.area_local_code}, area_local_name={self.area_local_name}, province_id={self.province_id}, "
                f"province_name={self.province_name}, province_code={self.province_code}, province_other_code={self.province_other_code}, "
                f"district_id={self.district_id}, district_name={self.district_name}, district_id_my_vnpt={self.district_id_my_vnpt}, "
                f"ward_id={self.ward_id}, ward_name={self.ward_name}, ward_id_my_vnpt={self.ward_id_my_vnpt}, co_tuyen_thu={self.co_tuyen_thu})")

