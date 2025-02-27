import logging
import traceback
from datetime import datetime

from helper.base_repository import BaseRepository

logger = logging.getLogger("Lakehouse")


class CertOrderRepository(BaseRepository):
    def __init__(self):
        super().__init__("cert_order")

    def count_by_time_and_locality(self, start_date: datetime = None, end_date: datetime = None,
                                   locality: str = None
                                   ):
        """Đếm số lượng đơn hàng"""
        try:
            end_year, end_month = end_date.year, end_date.month
            end_date_str = end_date.strftime("%Y-%m-%d %H:%M:%S")

            query = (f"SELECT COUNT(*) FROM {self.table_name} where created_date <= TIMESTAMP '{end_date_str}' "
                     f"and year_created <= {end_year} and month_created <= {end_month}")
            if start_date is not None:
                start_year, start_month = start_date.year, start_date.month
                start_date_str = start_date.strftime("%Y-%m-%d %H:%M:%S")
                query = (f"SELECT COUNT(*) FROM {self.table_name} "
                         f"where created_date BETWEEN TIMESTAMP '{start_date_str}' AND TIMESTAMP '{end_date_str}' ")
                if start_year == end_year and start_month == end_month:
                    query += f"and year_created = {end_year} AND month_created = {end_month}"
                else:
                    query += (f"and ((year_created = {start_year} AND month_created >= {start_month}) "
                              f"OR (year_created > {start_year} AND year_created < {end_year}) "
                              f"OR (year_created = {end_year} AND month_created <= {end_month}))")
            if locality:
                query += f" AND locality_code = '{locality}'"
            res = self.fetch_all(query, as_dataframe=False)
            return res
        except Exception:
            logger.error(traceback.format_exc())
        return 0

    def order_by_source(self, start_date, end_date, locality):
        """Đơn hàng theo kênh bán"""
        try:
            end_year, end_month = end_date.year, end_date.month
            end_date_str = end_date.strftime("%Y-%m-%d %H:%M:%S")

            start_year, start_month = start_date.year, start_date.month
            start_date_str = start_date.strftime("%Y-%m-%d %H:%M:%S")

            query = (f"SELECT count(distinct id) as quantity, source, SUM(CAST(price AS BIGINT)) as income "
                     f"FROM {self.table_name} o "
                     f"WHERE created_date BETWEEN TIMESTAMP '{start_date_str}' AND TIMESTAMP '{end_date_str}' ")

            if start_year == end_year and start_month == end_month:
                query += f"AND year_created = {end_year} AND month_created = {end_month}"
            else:
                query += (f"AND ((year_created = {start_year} AND month_created >= {start_month}) "
                          f"OR (year_created > {start_year} AND year_created < {end_year}) "
                          f"OR (year_created = {end_year} AND month_created <= {end_month}))")

            if locality:
                query += f" AND locality_code = '{locality}'"
            query += f" GROUP BY source"
            res = self.fetch_all(query, as_dataframe=True)
            return res
        except Exception:
            logger.error(traceback.format_exc())
        return 0

    def order_by_account_type(self, start_date, end_date, locality):
        """Đơn hàng theo đối tượng"""
        try:
            end_year, end_month = end_date.year, end_date.month
            end_date_str = end_date.strftime("%Y-%m-%d %H:%M:%S")

            start_year, start_month = start_date.year, start_date.month
            start_date_str = start_date.strftime("%Y-%m-%d %H:%M:%S")

            days = (end_date - start_date).days

            if 60 > days > 30:
                query_date = ("CAST(YEAR(o.created_date) AS VARCHAR) || '-W' || "
                              "LPAD(CAST(EXTRACT(WEEK FROM o.created_date) AS VARCHAR), 2, '0')")
            elif days >= 60:
                query_date = "date_format(o.created_date, '%Y-%m')"
            else:
                query_date = "date_format(o.created_date, '%Y-%m-%d')"

            sub_query = (f"SELECT {query_date} as created_time, "
                         f"u.account_type_desc as account_type, "
                         f"COUNT(DISTINCT o.id) AS quantity, "
                         f"o.locality_code, o.source "
                         f"FROM {self.table_name} o "
                         f"JOIN user_info u ON u.id = o.identity_id "
                         f"WHERE o.created_date BETWEEN TIMESTAMP '{start_date_str}' AND TIMESTAMP '{end_date_str}' ")

            if start_year == end_year and start_month == end_month:
                sub_query += f"AND o.year_created = {end_year} AND o.month_created = {end_month} "
            else:
                sub_query += (f"AND ((o.year_created = {start_year} AND o.month_created >= {start_month}) "
                              f"OR (o.year_created > {start_year} AND o.year_created < {end_year}) "
                              f"OR (o.year_created = {end_year} AND o.month_created <= {end_month})) ")

            if locality:
                sub_query += f"AND o.locality_code = '{locality}' "

            sub_query += f"GROUP BY {query_date}, u.account_type_desc, o.locality_code, o.source "
            query = (f"SELECT created_time, account_type, locality_code, source, "
                     f"SUM(CAST(quantity AS BIGINT)) AS total_quantity "
                     f"FROM ({sub_query}) grouped_data "
                     f"GROUP BY created_time, account_type, locality_code, source "
                     f"ORDER BY total_quantity DESC")

            res = self.fetch_all(query, as_dataframe=True)
            return res

        except Exception:
            logger.error(traceback.format_exc())

        return 0

    def order_by_status(self, start_date, end_date, locality):
        """Đơn hàng theo kênh bán"""
        try:
            end_year, end_month = end_date.year, end_date.month
            end_date_str = end_date.strftime("%Y-%m-%d %H:%M:%S")

            start_year, start_month = start_date.year, start_date.month
            start_date_str = start_date.strftime("%Y-%m-%d %H:%M:%S")

            query = (f"SELECT count(distinct id) as quantity, status_desc, type as type_order, source, locality_code "
                     f"FROM {self.table_name} "
                     f"WHERE created_date BETWEEN TIMESTAMP '{start_date_str}' AND TIMESTAMP '{end_date_str}' "
                     f"AND status NOT IN (99, 100) ")

            if start_year == end_year and start_month == end_month:
                query += f"AND year_created = {end_year} AND month_created = {end_month} "
            else:
                query += (f"AND ((year_created = {start_year} AND month_created >= {start_month}) "
                          f"OR (year_created > {start_year} AND year_created < {end_year}) "
                          f"OR (year_created = {end_year} AND month_created <= {end_month})) ")

            if locality:
                query += f" AND locality_code = '{locality}' "
            query += f" GROUP BY status_desc, type, source, locality_code"
            res = self.fetch_all(query, as_dataframe=True)
            return res
        except Exception:
            logger.error(traceback.format_exc())
        return 0
