"""parsers 单元测试 —— 基于 tests/case/ 的真实数据"""

from pathlib import Path

from crawler.fund_context import FundContext
from crawler.parsers import STEPS
from crawler.parsers.eastmoney import (
    build_overview_url, parse_overview,
    build_manager_url, parse_manager,
)
from crawler.parsers.morningstar import (
    build_morningstar_url, parse_morningstar,
    build_return_url, parse_return,
    build_risk_url, parse_risk,
)
from utils.constants import NO_DATA

CASE = Path(__file__).parent / "case"


def _read(filename: str) -> str:
    return (CASE / filename).read_text(encoding="utf-8")


class TestOverview:
    ctx: FundContext
    html: str

    def setup_method(self):
        self.ctx = FundContext("910009", "东方红启程三年持有混合A")
        self.html = _read("OVERVIEW.html")

    def test_build_url(self):
        assert "910009" in build_overview_url(self.ctx)

    def test_fund_type(self):
        parse_overview(self.html, self.ctx)
        assert self.ctx.fund_type == "混合型-偏股"

    def test_fund_size(self):
        parse_overview(self.html, self.ctx)
        assert self.ctx.fund_size == "4.55"

    def test_fund_company(self):
        parse_overview(self.html, self.ctx)
        assert self.ctx.fund_company == "东方红资产管理"

    def test_fund_value(self):
        parse_overview(self.html, self.ctx)
        assert self.ctx.fund_value == "7.1568"

    def test_management_fee(self):
        parse_overview(self.html, self.ctx)
        assert self.ctx.management_fee_rate == "0.80%"

    def test_custody_fee(self):
        parse_overview(self.html, self.ctx)
        assert self.ctx.custody_fee_rate == "0.20%"

    def test_sales_service_fee_dash(self):
        parse_overview(self.html, self.ctx)
        assert self.ctx.sales_service_fee_rate == NO_DATA  # "---"

    def test_none_html_does_nothing(self):
        parse_overview(None, self.ctx)
        assert self.ctx.fund_type is None


class TestManager:
    ctx: FundContext
    html: str

    def setup_method(self):
        self.ctx = FundContext("910009", "东方红启程三年持有混合A")
        self.html = _read("MANAGER.html")

    def test_build_url(self):
        assert "910009" in build_manager_url(self.ctx)

    def test_manager_name(self):
        parse_manager(self.html, self.ctx)
        assert self.ctx.fund_manager == "王焯"

    def test_appointment_date(self):
        parse_manager(self.html, self.ctx)
        assert self.ctx.date_of_appointment == "2026-02-14"

    def test_none_html_does_nothing(self):
        parse_manager(None, self.ctx)
        assert self.ctx.fund_manager is None


class TestMorningstar:
    ctx: FundContext
    json_text: str

    def setup_method(self):
        self.ctx = FundContext("000457", "测试基金")
        self.json_text = _read("MORNINGSTAR.json")

    def test_build_url(self):
        assert "000457" in build_morningstar_url(self.ctx)

    def test_fund_class_id(self):
        parse_morningstar(self.json_text, self.ctx)
        assert self.ctx.morningstar_fund_id == "0P00011NK0"

    def test_none_json_does_nothing(self):
        parse_morningstar(None, self.ctx)
        assert self.ctx.morningstar_fund_id is None


class TestReturn:
    ctx: FundContext
    json_text: str

    def setup_method(self):
        self.ctx = FundContext("000457", "测试基金")
        self.ctx.morningstar_fund_id = "0P00011NK0"
        self.json_text = _read("RETURN.json")

    def test_build_url(self):
        assert "0P00011NK0" in build_return_url(self.ctx)

    def test_five_year_return(self):
        parse_return(self.json_text, self.ctx)
        assert self.ctx.annualized_return_five_year == "7.34828"

    def test_ten_year_return(self):
        parse_return(self.json_text, self.ctx)
        assert self.ctx.annualized_return_ten_year == "11.63323"

    def test_none_json_does_nothing(self):
        parse_return(None, self.ctx)
        assert self.ctx.annualized_return_five_year is None


class TestRisk:
    ctx: FundContext
    json_text: str

    def setup_method(self):
        self.ctx = FundContext("000457", "测试基金")
        self.ctx.morningstar_fund_id = "0P00011NK0"
        self.json_text = _read("RISK.json")

    def test_build_url(self):
        assert "0P00011NK0" in build_risk_url(self.ctx)

    def test_std_dev_five(self):
        parse_risk(self.json_text, self.ctx)
        assert self.ctx.standard_deviation_five_years == "18.26328"

    def test_std_dev_ten_empty(self):
        parse_risk(self.json_text, self.ctx)
        assert self.ctx.standard_deviation_ten_years == NO_DATA  # Year10 is ""

    def test_sharp_five(self):
        parse_risk(self.json_text, self.ctx)
        assert self.ctx.sharp_rate_five_years == "0.44460"

    def test_sharp_ten_empty(self):
        parse_risk(self.json_text, self.ctx)
        assert self.ctx.sharp_rate_ten_years == NO_DATA  # Year10 is ""

    def test_alpha(self):
        parse_risk(self.json_text, self.ctx)
        assert self.ctx.alpha_to_ind == "5.46289"

    def test_beta(self):
        parse_risk(self.json_text, self.ctx)
        assert self.ctx.beta_to_ind == "1.46742"

    def test_r_squared(self):
        parse_risk(self.json_text, self.ctx)
        assert self.ctx.r_squared_to_ind == "49.72944"

    def test_null_json_fills_no_data(self):
        parse_risk("null", self.ctx)
        assert self.ctx.alpha_to_ind == NO_DATA
        assert self.ctx.beta_to_ind == NO_DATA

    def test_none_json_fills_no_data(self):
        parse_risk(None, self.ctx)
        assert self.ctx.alpha_to_ind == NO_DATA


class TestSTEPS:
    def test_all_steps_registered(self):
        names = {s.name for s in STEPS}
        assert names == {"overview", "manager", "morningstar", "return", "risk"}

    def test_no_dependency_steps(self):
        for s in STEPS:
            if s.name in ("overview", "manager", "morningstar"):
                assert s.deps == ()

    def test_morningstar_dependent_steps(self):
        by_name = {s.name: s for s in STEPS}
        assert by_name["return"].deps == ("morningstar",)
        assert by_name["risk"].deps == ("morningstar",)

    def test_each_step_has_build_url_and_parse(self):
        for s in STEPS:
            assert callable(s.build_url), f"{s.name} missing build_url"
            assert callable(s.parse), f"{s.name} missing parse"
