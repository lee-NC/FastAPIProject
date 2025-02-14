from helper.base_repository import BaseRepository


class CredentialRepository(BaseRepository):
    def __init__(self):
        super().__init__("CREDENTIAL")

    async def count_credential_by_user_ids(self, filters: str = None, user_ids: list = []):
        """Đếm số lượng credential của user theo user_ids và trạng thái"""
        def scan(table):
            active_users = set()
            for _, data in table.scan(filter=filters, columns=['INFO:IDENTITY_ID']):
                identity_id = data.get(b'INFO:IDENTITY_ID', b'').decode('utf-8')
                if identity_id in user_ids:
                    active_users.add(identity_id)
            return len(active_users)

        return await self.with_connection(scan)
