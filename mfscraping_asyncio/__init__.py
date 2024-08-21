import asyncio
import datetime
import json
import re
import urllib.parse
from dataclasses import dataclass
from typing import Any, TypeAlias, TypeGuard

import aiohttp
from bs4 import BeautifulSoup as BS
from bs4 import Tag

from .exceptions import (
    DataDoesNotExist,
    FetchTimeout,
    LoginFailed,
    MFConnectionError,
    MFInitializeError,
    MFScraptingError,
    NeedOTP,
)

Account: TypeAlias = tuple[str] | tuple[str, str]


def is_Account(x: Any) -> TypeGuard[Account]:
    if isinstance(x, tuple):
        match len(x):
            case 1:
                return isinstance(x[0], str)
            case 2:
                return isinstance(x[0], str) and isinstance(x[1], str)
            case _:
                return False
    else:
        return False


def str2Account(x: str) -> Account:
    x_ = x.split(":")
    return (x_[0], x_[1]) if len(x_) == 2 else (x_[0],)


def Account2str(x: Account) -> str:
    return ":".join(x)


@dataclass
class MFTransaction:
    transaction_id: int
    date: datetime.date
    amount: int
    account: Account | tuple[Account, Account]
    lcategory: str = "未分類"
    mcategory: str = "未分類"
    content: str = ""
    memo: str = ""

    def __lt__(self, other: Any):
        if not isinstance(other, MFTransaction):
            return NotImplemented
        if self.date == other.date:
            return self.transaction_id < other.transaction_id
        else:
            return self.date < other.date

    def __le__(self, other: Any):
        return self.__lt__(other) or self.__eq__(other)

    def __gt__(self, other: Any):
        return not self.__le__(other)

    def __ge__(self, other: Any):
        return not self.__lt__(other)

    def _inner_is_transfer(
        self, ac: Account | tuple[Account, Account]
    ) -> TypeGuard[tuple[Account, Account]]:
        return not isinstance(ac[0], str)

    def is_transfer(self) -> bool:
        return self._inner_is_transfer(self.account)

    def account_from(self) -> Account:
        if self._inner_is_transfer(self.account):
            return self.account[0]
        else:
            raise ValueError()

    def account_to(self) -> Account:
        if self._inner_is_transfer(self.account):
            return self.account[1]
        else:
            raise ValueError()


class MFScraper:
    def __init__(self, id: int, passwd: str, timeout: int = 10) -> None:
        self._id = id
        self._passwd = passwd
        self._timeout = timeout
        self._session = None
        self._account = None
        self._category = None
        self._headers = {}
        self._otp_post_data = None
        self._is_logined = False

    async def __aenter__(self):
        self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(self._timeout))
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self._session:
            await self._session.close()

    async def _get(self, url: str) -> str:
        if not self._session:
            raise MFInitializeError()
        try:
            async with self._session.get(url, headers=self._headers) as result:
                result.raise_for_status()
                return await result.text()
        except (aiohttp.ServerTimeoutError, aiohttp.ClientResponseError) as e:
            raise MFConnectionError(e)

    async def _post(self, url: str, post_data: dict | None, is_text: bool) -> str:
        if not self._session:
            raise MFInitializeError()
        try:
            async with self._session.post(url, data=post_data, headers=self._headers) as result:
                result.raise_for_status()
                return await result.text() if is_text else ""
        except (aiohttp.ServerTimeoutError, aiohttp.ClientResponseError) as e:
            raise MFConnectionError(e)

    async def _put(self, url: str, put_data: dict) -> None:
        if not self._session:
            raise MFInitializeError()
        try:
            async with self._session.put(url, params=put_data, headers=self._headers) as result:
                result.raise_for_status()
        except (aiohttp.ServerTimeoutError, aiohttp.ClientResponseError) as e:
            raise MFConnectionError(e)

    async def _delete(self, url: str) -> None:
        if not self._session:
            raise MFInitializeError()
        try:
            async with self._session.delete(url, headers=self._headers) as result:
                result.raise_for_status()
        except (aiohttp.ServerTimeoutError, aiohttp.ClientResponseError) as e:
            raise MFConnectionError(e)

    async def login(self) -> None:
        if not self._session:
            raise MFInitializeError()
        try:
            async with self._session.get("https://moneyforward.com/sign_in/") as result:
                result.raise_for_status()
                qs = urllib.parse.urlparse(str(result.url)).query
                qs_d = urllib.parse.parse_qs(qs)
                ret = await result.text()
        except (aiohttp.ServerTimeoutError, aiohttp.ClientResponseError) as e:
            raise MFConnectionError(e)
        soup = BS(ret, "html.parser")
        tmp = soup.select_one("meta[name=csrf-token]")
        if isinstance(tmp, Tag):
            token = tmp.get("content")
        else:
            raise MFScraptingError()
        post_data = {
            "authenticity_token": token,
            "_method": "post",
            "mfid_user[email]": self._id,
            "mfid_user[password]": self._passwd,
            "select_account": "true",
        }
        post_data.update(qs_d)
        try:
            async with self._session.post(
                "https://id.moneyforward.com/sign_in", data=post_data
            ) as result:
                result.raise_for_status()
                tmp = str(result.url)
                if tmp == "https://moneyforward.com/":
                    soup = BS(await result.text(), "html.parser")
                    tmp = soup.select_one("meta[name=csrf-token]")
                    if isinstance(tmp, Tag):
                        self._headers = {
                            "X-CSRF-Token": tmp.get("content"),
                            "X-Requested-With": "XMLHttpRequest",
                        }
                    else:
                        raise MFScraptingError()
                    self._is_logined = True
                elif "email_otp" in tmp:
                    tmp = re.search(r"gon\.authorizationParams={.*?}", await result.text())
                    if tmp:
                        tmp = tmp.group().replace("gon.authorizationParams=", "")
                        post_data = json.loads(tmp)
                    soup = BS(ret, "html.parser")
                    tmp = soup.select_one("meta[name=csrf-token]")
                    if isinstance(tmp, Tag):
                        token = tmp.get("content")
                    post_data["authenticity_token"] = token
                    post_data["method"] = "post"
                    self._otp_post_data = post_data
                    raise NeedOTP
                else:
                    raise LoginFailed
        except (aiohttp.ServerTimeoutError, aiohttp.ClientResponseError) as e:
            raise MFConnectionError(e)

    async def login_otp(self, otp) -> None:
        if not self._session:
            raise MFInitializeError()
        try:
            if self._is_logined:
                return
            if not self._otp_post_data:
                raise LoginFailed
            self._otp_post_data["email_otp"] = otp
            async with self._session.post(
                "https://id.moneyforward.com/email_otp", data=self._otp_post_data
            ) as result:
                result.raise_for_status()
                tmp = str(result.url)
                if tmp == "https://moneyforward.com/":
                    soup = BS(await result.text(), "html.parser")
                    tmp = soup.select_one("meta[name=csrf-token]")
                    if isinstance(tmp, Tag):
                        self._headers = {
                            "X-CSRF-Token": tmp.get("content"),
                            "X-Requested-With": "XMLHttpRequest",
                        }
                    else:
                        raise MFScraptingError()
                    self._is_logined = True
                else:
                    raise LoginFailed
        except (aiohttp.ServerTimeoutError, aiohttp.ClientResponseError) as e:
            raise MFConnectionError(e)

    async def fetch(self, delay: int = 2, maxwaiting: int = 300, delta=60) -> None:
        ret = await self._get("https://moneyforward.com")
        soup = BS(ret, "html.parser")
        urls = soup.select("a[data-remote=true]")
        for url in urls:
            tmp = url
            skip = False
            for _ in range(3):
                if tmp is None:
                    skip = True
                    break
                tmp = tmp.parent
            if skip or tmp is None:
                continue
            tmp = tmp.select_one(".date")
            if tmp is None:
                continue
            m = re.search(r"\((.*)\)", tmp.text)
            if m is None:
                continue
            date_str = m.group(1)
            now = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=9)))
            tmp = date_str.split(" ")
            tmp1 = tmp[0].split("/")
            tmp2 = tmp[1].split(":")
            update_date = datetime.datetime(
                now.year,
                int(tmp1[0]),
                int(tmp1[1]),
                int(tmp2[0]),
                int(tmp2[1]),
                tzinfo=datetime.timezone(datetime.timedelta(hours=9)),
            )
            if now < update_date or now >= update_date + datetime.timedelta(minutes=delta):
                await self._post("https://moneyforward.com" + str(url["href"]), None, False)
        counter = 0
        while counter < maxwaiting:
            await asyncio.sleep(delay)
            counter += delay
            j = await self._get("https://moneyforward.com/accounts/polling")
            if not json.loads(j)["loading"]:
                return
        raise FetchTimeout

    async def get(self, year: int, month: int) -> list[MFTransaction]:
        post_data = {
            "from": str(year) + "/" + str(month) + "/1",
            "service_id": "",
            "account_id_hash": "",
        }
        text = await self._post("https://moneyforward.com/cf/fetch", post_data, True)
        search_result = re.search(r'\$\("\.list_body"\)\.append\((.*?)\);', text)
        if search_result is None:
            raise DataDoesNotExist
        html = search_result.group(1)
        html = eval(html).replace("\\", "")
        soup = BS(html, "html.parser")
        trs = soup.select("tr")
        ret: list[MFTransaction] = []
        for tr in trs:
            if "icon-ban-circle" in str(tr):
                continue
            transaction_id = int(str(tr["id"]).replace("js-transaction-", ""))
            if (tmp := tr.select_one("td.date")) is None:
                raise MFScraptingError
            td_date = tmp.text.replace("\n", "")
            date = datetime.date(year, int(td_date[0:2]), int(td_date[3:5]))
            if (tmp := tr.select_one("td.amount")) is None:
                raise MFScraptingError
            td_amount = tmp.text.replace("\n", "")
            is_transfer = "振替" in td_amount
            amount = int(re.sub("[^0-9-]", "", td_amount))
            if (td_calc := tr.select_one("td.calc[style]")) is None:
                raise MFScraptingError
            for sel in td_calc.select("select"):
                sel.clear()
            if is_transfer:
                if (tmp := td_calc.select_one("div.transfer_account_box")) is None:
                    raise MFScraptingError
                if (to := tmp.extract()) is None:
                    raise MFScraptingError
                acs = [td_calc.text.replace("\n", ""), to.text.replace("\n", "")]
                subacs = str(td_calc["title"]).split("から")
                subacs[0] = subacs[0].replace(acs[0], "", 1).strip()
                subacs[1] = subacs[1].replace(acs[1], "", 1).replace("への振替", "").strip()
                account = (
                    (acs[0], subacs[0]) if subacs[0] != "" else (acs[0],),
                    (acs[1], subacs[1]) if subacs[1] != "" else (acs[1],),
                )
            else:
                ac = td_calc.text.replace("\n", "")
                subac = str(td_calc["title"]).replace(ac, "", 1).strip()
                account = (ac, subac) if subac != "" else (ac,)
            if (tdlctg := tr.select_one("td.lctg")) is None:
                raise MFScraptingError
            if (tdmctg := tr.select_one("td.mctg")) is None:
                raise MFScraptingError
            if (tdcontent := tr.select_one("td.content")) is None:
                raise MFScraptingError
            if (tdmemo := tr.select_one("td.memo")) is None:
                raise MFScraptingError
            ret.append(
                MFTransaction(
                    transaction_id,
                    date,
                    abs(amount) if is_transfer else amount,
                    account,
                    tdlctg.text.replace("\n", ""),
                    tdmctg.text.replace("\n", ""),
                    tdcontent.text.replace("\n", ""),
                    tdmemo.text.replace("\n", ""),
                )
            )
        ret = sorted(ret, reverse=True)
        return ret

    async def get_account(self) -> dict[Account, dict[str, str]]:
        async def inner_get_account(self: MFScraper) -> dict[Account, dict[str, str]]:
            ret = await self._get("https://moneyforward.com/groups")
            soup = BS(ret, "html.parser")
            if (tmp := soup.select_one(".edit > a")) is None:
                raise MFScraptingError
            url = str(tmp["href"])
            ret = await self._get("https://moneyforward.com" + url)
            soup = BS(ret, "html.parser")
            accounts: dict[Account, dict[str, str]] = {}
            for a in soup.select(".js-sub-account-group-parent"):
                account_id = str(a["id"]).replace("js-sub_account_split_", "")
                aname = str(a.next_sibling).replace("\n", "")
                sub_accounts = soup.select("." + re.sub("^([1-9])", "\\\\3\\1 ", account_id))
                if sub_accounts:
                    for sa in sub_accounts:
                        if sa.has_attr("checked"):
                            saname = " ".join(
                                re.sub(
                                    "^\\s|\\s$", "", str(sa.next_sibling).replace("\n", "")
                                ).split()
                            )
                            tmp = {
                                "account_id": account_id,
                                "sub_account_id": sa["value"],
                            }
                            accounts.update({(aname, saname): tmp})
                else:
                    if a.has_attr("checked"):
                        tmp = {
                            "account_id": account_id,
                            "sub_account_id": a["value"],
                        }
                        accounts.update({(aname,): tmp})
            return accounts

        if not self._account:
            self._account = asyncio.create_task(inner_get_account(self))
        return await self._account

    async def get_category(self) -> dict[tuple[str, str, str], dict[str, int]]:
        async def inner_get_category(
            self: MFScraper,
        ) -> dict[tuple[str, str, str], dict[str, int]]:
            ret = await self._get("https://moneyforward.com/cf")
            soup = BS(ret, "html.parser")
            categories: dict[tuple[str, str, str], dict[str, int]] = {}
            css_list = ["ul.dropdown-menu.main_menu.plus", "ul.dropdown-menu.main_menu.minus"]
            keys = ["plus", "minus"]
            for css, key in zip(css_list, keys):
                c_pm = soup.select_one(css)
                if c_pm:
                    for l_c in c_pm.select("li.dropdown-submenu"):
                        tmp = l_c.select_one("a.l_c_name")
                        lname = tmp.text if tmp else ""
                        lid = int(str(tmp["id"])) if tmp else 0
                        for m_c in l_c.select("a.m_c_name"):
                            mname = m_c.text
                            mid = int(str(m_c["id"])) if m_c else 0
                            categories.update({(key, lname, mname): {"lid": lid, "mid": mid}})
            return categories

        if not self._category:
            self._category = asyncio.create_task(inner_get_category(self))
        return await self._category

    async def save(self, data: MFTransaction) -> None:
        categories = await self.get_category()
        date_str = data.date.strftime("%Y/%m/%d")
        accounts = await self.get_account()
        post_data = {
            "user_asset_act[updated_at]": date_str,
            "user_asset_act[recurring_flag]": 0,
            "user_asset_act[amount]": abs(data.amount),
            "user_asset_act[content]": data.content,
            "commit": "保存する",
        }
        if not is_Account(data.account):
            ac_id_from = accounts[data.account_from()]["sub_account_id"]
            ac_id_to = accounts[data.account_to()]["sub_account_id"]
            post_data_add = {
                "user_asset_act[is_transfer]": 1,
                "user_asset_act[sub_account_id_hash_from]": ac_id_from,
                "user_asset_act[sub_account_id_hash_to]": ac_id_to,
            }
            post_data.update(post_data_add)
        else:
            if data.amount > 0:
                is_income = 1
                tmp = categories[("plus", data.lcategory, data.mcategory)]
            else:
                is_income = 0
                tmp = categories[("minus", data.lcategory, data.mcategory)]
            l_c_id = tmp["lid"]
            m_c_id = tmp["mid"]
            ac_id = accounts[data.account]["sub_account_id"]
            post_data_add = {
                "user_asset_act[is_transfer]": 0,
                "user_asset_act[is_income]": is_income,
                "user_asset_act[sub_account_id_hash]": ac_id,
                "user_asset_act[large_category_id]": l_c_id,
                "user_asset_act[middle_category_id]": m_c_id,
            }
            post_data.update(post_data_add)
        await self._post("https://moneyforward.com/cf/create", post_data, False)

    async def update(self, data: MFTransaction) -> None:
        if not is_Account(data.account):
            raise ValueError()
        categories = await self.get_category()
        accounts = await self.get_account()
        put_data = {
            "user_asset_act[id]": data.transaction_id,
            "user_asset_act[table_name]": "user_asset_act",
        }
        date_str = data.date.strftime("%Y/%m/%d")
        put_data.update({"user_asset_act[updated_at]": date_str})
        put_data.update({"user_asset_act[amount]": data.amount})
        put_data.update({"user_asset_act[content]": data.content})
        put_data.update({"user_asset_act[memo]": data.memo})
        if data.amount > 0:
            is_income = 1
            tmp = categories[("plus", data.lcategory, data.mcategory)]
        else:
            is_income = 0
            tmp = categories[("minus", data.lcategory, data.mcategory)]
        l_c_id = tmp["lid"]
        m_c_id = tmp["mid"]
        put_data.update({"user_asset_act[is_income]": is_income})
        put_data.update({"user_asset_act[large_category_id]": l_c_id})
        put_data.update({"user_asset_act[middle_category_id]": m_c_id})
        ac_id = accounts[data.account]["sub_account_id"]
        put_data.update({"user_asset_act[sub_account_id_hash]": ac_id})
        await self._put("https://moneyforward.com/cf/update", put_data)

    async def transfer(
        self,
        data: MFTransaction,
        partner_data: MFTransaction | None = None,
        partner_account: Account | None = None,
    ) -> None:
        accounts = await self.get_account()
        if partner_data:
            if is_Account(partner_data.account):
                tmp = accounts[partner_data.account]
            else:
                raise ValueError()
        elif partner_account:
            tmp = accounts[partner_account]
        else:
            raise ValueError()
        post_data = {
            "_method": "put",
            "user_asset_act[id]": data.transaction_id,
            "user_asset_act[partner_account_id_hash]": tmp["account_id"],
            "user_asset_act[partner_sub_account_id_hash]": tmp["sub_account_id"],
            "commit": "設定を保存",
        }
        if partner_data is not None:
            post_data.update({"user_asset_act[partner_act_id]": partner_data.transaction_id})
        await self.enable_transfer(data)
        await self._post("https://moneyforward.com/cf/update", post_data, False)

    async def enable_transfer(self, data: MFTransaction) -> None:
        await self._put(
            "https://moneyforward.com/cf/update.js",
            {"change_type": "enable_transfer", "id": data.transaction_id},
        )

    async def disable_transfer(self, data: MFTransaction) -> None:
        await self._put(
            "https://moneyforward.com/cf/update.js",
            {"change_type": "disable_transfer", "id": data.transaction_id},
        )

    async def delete(self, data: MFTransaction) -> None:
        await self._delete("https://moneyforward.com/cf/" + str(data.transaction_id))

    async def get_withdrawal(self) -> dict[Account, dict[str, int | datetime.date]]:
        accounts = await self.get_account()
        ids = set(x["account_id"] for x in accounts.values())
        ret_ = await self._get("https://moneyforward.com")
        soup_ = BS(ret_, "html.parser")
        ret = {}
        dt_now_jst = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=9)))
        for text, id in zip(
            await asyncio.gather(
                *[self._get("https://moneyforward.com/accounts/show/" + id) for id in ids]
            ),
            ids,
        ):
            soup = BS(text, "html.parser")
            table = soup.select_one(".table-bordered")
            title = soup.select_one(".show-title")
            update_date_str1 = soup_.select_one("div.date." + re.sub("^([1-9])", "\\\\3\\1 ", id))
            update_date_str2 = soup_.select_one(
                "div.date-not-display-none." + re.sub("^([1-9])", "\\\\3\\1 ", id)
            )
            update_date_str = (
                update_date_str1 if update_date_str1 is not None else update_date_str2
            )
            if table and title and update_date_str:
                if table.select("thead tr th")[3].text == "引き落とし予定額":
                    update_date_md = (
                        update_date_str.text.replace("取得日時(", "").split(" ")[0].split("/")
                    )
                    update_date = datetime.date(
                        dt_now_jst.year, int(update_date_md[0]), int(update_date_md[1])
                    )
                    if update_date > dt_now_jst.date():
                        update_date = datetime.date(
                            dt_now_jst.year - 1, int(update_date_md[0]), int(update_date_md[1])
                        )
                    for tr in table.select("tbody tr"):
                        tds = tr.select("td")
                        subac = (
                            tds[1].text.replace("\n", "") + " " + tds[2].text.replace("\n", "")
                        ).strip()
                        if (amount_date := tds[3].text.replace("\n", "")) != "-":
                            amount_date = amount_date.split("(")
                            amount = int(amount_date[0].replace(",", "").replace("円", ""))
                            date_str = amount_date[1].replace(")", "").split("/")
                            date = datetime.date(
                                int(date_str[0]), int(date_str[1]), int(date_str[2])
                            )
                        else:
                            amount = None
                            date = None
                        ret.update(
                            {
                                (title.text, subac): {
                                    "amount": amount,
                                    "date": date,
                                    "update_date": update_date,
                                }
                            }
                        )
        return ret

    async def get_balance(self) -> dict[Account, dict[str, int | datetime.date]]:
        accounts = await self.get_account()
        ids = set(x["account_id"] for x in accounts.values())
        ret_ = await self._get("https://moneyforward.com")
        soup_ = BS(ret_, "html.parser")
        ret = {}
        dt_now_jst = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=9)))
        for text, id in zip(
            await asyncio.gather(
                *[self._get("https://moneyforward.com/accounts/show/" + id) for id in ids]
            ),
            ids,
        ):
            soup = BS(text, "html.parser")
            table = soup.select_one(".table-bordered")
            title = soup.select_one(".show-title")
            update_date_str = soup_.select_one("div.date." + re.sub("^([1-9])", "\\\\3\\1 ", id))
            if table and title and update_date_str:
                if table.select("thead tr th")[3].text == "残高":
                    title_text = re.sub(r"\([^()]*\)", "", title.text.replace("\n", ""))
                    update_date_md = (
                        update_date_str.text.replace("取得日時(", "").split(" ")[0].split("/")
                    )
                    update_date = datetime.date(
                        dt_now_jst.year, int(update_date_md[0]), int(update_date_md[1])
                    )
                    if update_date > dt_now_jst.date():
                        update_date = datetime.date(
                            dt_now_jst.year - 1, int(update_date_md[0]), int(update_date_md[1])
                        )
                    amount = 0
                    for tr in table.select("tbody tr"):
                        if isinstance(li := tr.get("class"), list) and "outside-group" in li:
                            continue
                        tds = tr.select("td")
                        if (tmp := tds[3].text.replace("\n", "")) != "-":
                            amount += int(tmp.replace(",", "").replace("円", ""))
                    ret.update({(title_text,): {"amount": amount, "update_date": update_date}})
        return ret
