from chains.intent import classify_intent


def test_classify_intent_routes_filing_summary_queries():
    assert classify_intent("Summarize Elliott's latest filing") == "filing_summary"


def test_classify_intent_routes_holdings_analysis_queries():
    assert classify_intent("What positions does SIR hold in tech?") == "holdings_analysis"


def test_classify_intent_routes_nl_query_questions():
    assert classify_intent("How many managers hold AAPL?") == "nl_query"


def test_classify_intent_defaults_ambiguous_queries_to_rag_search():
    assert classify_intent("What do our research notes say about Elliott?") == "rag_search"
    assert classify_intent("Tell me more") == "rag_search"
