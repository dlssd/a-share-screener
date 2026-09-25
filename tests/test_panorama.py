from app.sync import classify_limit_board


def test_strict_t_board_and_one_word_labels():
    t=classify_limit_board(11,11,10.6,11,11,2)
    assert t=={"board_label":"2板","is_t_board":True,"is_one_word":False}
    one=classify_limit_board(11,11,11,11,11,4)
    assert one["board_label"]=="4板及以上"
    assert one["is_one_word"] and not one["is_t_board"]


def test_non_limit_ohlc_is_not_force_classified():
    result=classify_limit_board(10.8,11,10.5,10.9,11,1)
    assert not result["is_t_board"] and not result["is_one_word"]
