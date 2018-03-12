-module(sql92).
-export([parse/1]).
parse(<<Bin/binary>>) -> parse(binary_to_list(Bin));
parse(Q) ->
  P = case sql92_scan:string(Q) of
    {ok, T, _} -> 
      case sql92_parser:parse(T) of
        {ok, [P_]} -> P_;
        PE_ -> PE_
      end;
    LE_ -> LE_
  end,
  P.