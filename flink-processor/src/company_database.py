"""
Company Database for Supply Chain Risk Predictor

This module contains mappings of company names/keywords to their stock tickers,
focusing on Fortune 500 companies and major global suppliers.

Structure:
- Primary company names (official names)
- Common aliases and abbreviations
- Major subsidiaries and brands
- Supply chain partners
"""

from typing import Dict, List, Set
from dataclasses import dataclass

@dataclass
class CompanyInfo:
    """Company information for enrichment matching."""
    ticker: str
    official_name: str
    keywords: List[str]  # All possible matching keywords
    sector: str
    is_supplier: bool = False  # True if primarily a supplier to other companies

# Fortune 500 + Major Global Companies Database
COMPANY_DATABASE = {
    # Technology - FAANG+
    "AAPL": CompanyInfo(
        ticker="AAPL",
        official_name="Apple Inc.",
        keywords=["apple", "iphone", "ipad", "mac", "macbook", "ios", "app store", "foxconn", "hon hai"],
        sector="Technology"
    ),
    "GOOGL": CompanyInfo(
        ticker="GOOGL", 
        official_name="Alphabet Inc.",
        keywords=["google", "alphabet", "android", "youtube", "gmail", "chrome", "google cloud"],
        sector="Technology"
    ),
    "MSFT": CompanyInfo(
        ticker="MSFT",
        official_name="Microsoft Corporation", 
        keywords=["microsoft", "windows", "office", "xbox", "azure", "teams", "surface"],
        sector="Technology"
    ),
    "AMZN": CompanyInfo(
        ticker="AMZN",
        official_name="Amazon.com Inc.",
        keywords=["amazon", "aws", "prime", "alexa", "whole foods", "kindle"],
        sector="Technology"
    ),
    "META": CompanyInfo(
        ticker="META",
        official_name="Meta Platforms Inc.",
        keywords=["meta", "facebook", "instagram", "whatsapp", "oculus", "metaverse"],
        sector="Technology"
    ),
    "NFLX": CompanyInfo(
        ticker="NFLX",
        official_name="Netflix Inc.",
        keywords=["netflix"],
        sector="Technology"
    ),
    
    # Electric Vehicles & Automotive
    "TSLA": CompanyInfo(
        ticker="TSLA",
        official_name="Tesla Inc.",
        keywords=["tesla", "model s", "model 3", "model x", "model y", "cybertruck", "supercharger"],
        sector="Automotive"
    ),
    "GM": CompanyInfo(
        ticker="GM",
        official_name="General Motors Company",
        keywords=["general motors", "gm", "chevrolet", "cadillac", "buick", "gmc"],
        sector="Automotive"
    ),
    "F": CompanyInfo(
        ticker="F",
        official_name="Ford Motor Company",
        keywords=["ford", "f-150", "mustang", "lincoln"],
        sector="Automotive"
    ),
    "TM": CompanyInfo(
        ticker="TM",
        official_name="Toyota Motor Corporation",
        keywords=["toyota", "lexus", "prius", "camry", "corolla"],
        sector="Automotive"
    ),
    
    # Semiconductors & Hardware (Critical Supply Chain)
    "TSM": CompanyInfo(
        ticker="TSM",
        official_name="Taiwan Semiconductor Manufacturing Company",
        keywords=["tsmc", "taiwan semiconductor"],
        sector="Semiconductors",
        is_supplier=True
    ),
    "NVDA": CompanyInfo(
        ticker="NVDA",
        official_name="NVIDIA Corporation",
        keywords=["nvidia", "geforce", "rtx", "cuda"],
        sector="Semiconductors"
    ),
    "INTC": CompanyInfo(
        ticker="INTC",
        official_name="Intel Corporation",
        keywords=["intel", "xeon", "core", "pentium"],
        sector="Semiconductors"
    ),
    "AMD": CompanyInfo(
        ticker="AMD",
        official_name="Advanced Micro Devices Inc.",
        keywords=["amd", "ryzen", "radeon", "epyc"],
        sector="Semiconductors"
    ),
    "QCOM": CompanyInfo(
        ticker="QCOM",
        official_name="QUALCOMM Incorporated",
        keywords=["qualcomm", "snapdragon"],
        sector="Semiconductors"
    ),
    "AVGO": CompanyInfo(
        ticker="AVGO",
        official_name="Broadcom Inc.",
        keywords=["broadcom"],
        sector="Semiconductors",
        is_supplier=True
    ),
    
    # Major Suppliers & Manufacturing
    "HON": CompanyInfo(
        ticker="HON",
        official_name="Honeywell International Inc.",
        keywords=["honeywell"],
        sector="Industrial",
        is_supplier=True
    ),
    "GE": CompanyInfo(
        ticker="GE",
        official_name="General Electric Company",
        keywords=["general electric", "ge"],
        sector="Industrial",
        is_supplier=True
    ),
    "MMM": CompanyInfo(
        ticker="MMM",
        official_name="3M Company",
        keywords=["3m"],
        sector="Industrial",
        is_supplier=True
    ),
    "CAT": CompanyInfo(
        ticker="CAT",
        official_name="Caterpillar Inc.",
        keywords=["caterpillar", "cat", "heavy machinery"],
        sector="Industrial",
        is_supplier=True
    ),
    
    # Retail & Consumer
    "WMT": CompanyInfo(
        ticker="WMT",
        official_name="Walmart Inc.",
        keywords=["walmart", "sam's club"],
        sector="Retail"
    ),
    "TGT": CompanyInfo(
        ticker="TGT",
        official_name="Target Corporation",
        keywords=["target"],
        sector="Retail"
    ),
    "HD": CompanyInfo(
        ticker="HD",
        official_name="The Home Depot Inc.",
        keywords=["home depot"],
        sector="Retail"
    ),
    "COST": CompanyInfo(
        ticker="COST",
        official_name="Costco Wholesale Corporation",
        keywords=["costco"],
        sector="Retail"
    ),
    
    # Energy & Oil
    "XOM": CompanyInfo(
        ticker="XOM",
        official_name="Exxon Mobil Corporation",
        keywords=["exxon", "mobil", "exxon mobil"],
        sector="Energy"
    ),
    "CVX": CompanyInfo(
        ticker="CVX",
        official_name="Chevron Corporation",
        keywords=["chevron"],
        sector="Energy"
    ),
    
    # Financial Services
    "JPM": CompanyInfo(
        ticker="JPM",
        official_name="JPMorgan Chase & Co.",
        keywords=["jpmorgan", "chase", "jp morgan"],
        sector="Financial"
    ),
    "BAC": CompanyInfo(
        ticker="BAC",
        official_name="Bank of America Corporation",
        keywords=["bank of america"],
        sector="Financial"
    ),
    "WFC": CompanyInfo(
        ticker="WFC",
        official_name="Wells Fargo & Company",
        keywords=["wells fargo"],
        sector="Financial"
    ),
    
    # Healthcare & Pharmaceuticals
    "JNJ": CompanyInfo(
        ticker="JNJ",
        official_name="Johnson & Johnson",
        keywords=["johnson & johnson", "j&j"],
        sector="Healthcare"
    ),
    "PFE": CompanyInfo(
        ticker="PFE",
        official_name="Pfizer Inc.",
        keywords=["pfizer"],
        sector="Healthcare"
    ),
    "UNH": CompanyInfo(
        ticker="UNH",
        official_name="UnitedHealth Group Incorporated",
        keywords=["unitedhealth", "united health"],
        sector="Healthcare"
    ),
    
    # Aerospace & Defense
    "BA": CompanyInfo(
        ticker="BA",
        official_name="The Boeing Company",
        keywords=["boeing", "737", "747", "787"],
        sector="Aerospace"
    ),
    "LMT": CompanyInfo(
        ticker="LMT",
        official_name="Lockheed Martin Corporation",
        keywords=["lockheed martin", "lockheed"],
        sector="Aerospace",
        is_supplier=True
    ),
    "RTX": CompanyInfo(
        ticker="RTX",
        official_name="Raytheon Technologies Corporation",
        keywords=["raytheon", "pratt whitney"],
        sector="Aerospace",
        is_supplier=True
    ),
    
    # Telecommunications
    "VZ": CompanyInfo(
        ticker="VZ",
        official_name="Verizon Communications Inc.",
        keywords=["verizon"],
        sector="Telecommunications"
    ),
    "T": CompanyInfo(
        ticker="T",
        official_name="AT&T Inc.",
        keywords=["at&t", "att"],
        sector="Telecommunications"
    ),
    
    # Food & Beverage
    "KO": CompanyInfo(
        ticker="KO",
        official_name="The Coca-Cola Company",
        keywords=["coca-cola", "coke", "coca cola"],
        sector="Consumer Goods"
    ),
    "PEP": CompanyInfo(
        ticker="PEP",
        official_name="PepsiCo Inc.",
        keywords=["pepsi", "pepsico", "frito-lay", "gatorade"],
        sector="Consumer Goods"
    ),
    "MCD": CompanyInfo(
        ticker="MCD",
        official_name="McDonald's Corporation",
        keywords=["mcdonalds", "mcdonald's"],
        sector="Consumer Goods"
    ),
    
    # International/Chinese Companies (Major Supply Chain Players)
    "BABA": CompanyInfo(
        ticker="BABA",
        official_name="Alibaba Group Holding Limited",
        keywords=["alibaba", "taobao", "tmall"],
        sector="Technology"
    ),
    "TCEHY": CompanyInfo(
        ticker="TCEHY",
        official_name="Tencent Holdings Limited",
        keywords=["tencent", "wechat", "qq"],
        sector="Technology"
    ),
    
    # Major Suppliers (Non-public but important for supply chain)
    # Note: These would map to public parent companies or ETFs
    # Foxconn impacts are mapped to Apple since it's Apple's primary supplier
}

def build_keyword_mapping() -> Dict[str, str]:
    """
    Build a flattened keyword -> ticker mapping for fast lookup.
    
    Returns:
        Dict mapping lowercase keywords to stock tickers
    """
    keyword_map = {}
    
    for ticker, company_info in COMPANY_DATABASE.items():
        # Add all keywords for this company
        for keyword in company_info.keywords:
            keyword_lower = keyword.lower()
            if keyword_lower in keyword_map:
                # Handle conflicts - prefer the more specific mapping
                # For now, just warn and keep the first one
                print(f"Warning: Keyword '{keyword_lower}' maps to both {keyword_map[keyword_lower]} and {ticker}")
            else:
                keyword_map[keyword_lower] = ticker
    
    return keyword_map

def get_company_info(ticker: str) -> CompanyInfo:
    """Get company information by ticker."""
    return COMPANY_DATABASE.get(ticker)

def get_suppliers() -> List[CompanyInfo]:
    """Get all companies marked as suppliers."""
    return [info for info in COMPANY_DATABASE.values() if info.is_supplier]

def get_companies_by_sector(sector: str) -> List[CompanyInfo]:
    """Get all companies in a specific sector."""
    return [info for info in COMPANY_DATABASE.values() if info.sector == sector]

# Pre-build the keyword mapping for performance
KEYWORD_TO_TICKER = build_keyword_mapping()

# Export the main mapping for backward compatibility
def get_company_keywords() -> Dict[str, str]:
    """Get the keyword to ticker mapping (legacy interface)."""
    return KEYWORD_TO_TICKER.copy()
