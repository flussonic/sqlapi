-module(sqlapi).

-include("../include/sqlapi.hrl").
-include("myproto.hrl").


-export([start_server/1, stop_server/1, existing_port/1, load_config/1]).

-export([password_hash/2, execute/3, terminate/2]).
-export([create_temporary_table/2, drop_temporary_table/1]).


-export([notify/2]).


-export([apply_sql_filter/2, arith/3]).
-export([mk_cond/1, filter/2, sqlite_flt/1, find_constraint/2]).

-define(ERR_WRONG_PASS, {error, <<"Password incorrect!">>}).
-define(ERR_WRONG_USER, {error, <<"No such user!">>}).
-define(ERR_LOGIN_DISABLED, {error, <<"Login disabled">>}).
-define(ERR_INFO(Code, Desc), #response{status=?STATUS_ERR, error_code=Code, info=Desc}).

-define(field(Key,Dict), proplists:get_value(Key,Dict)).

-define(COUNT_COL_NAME, ' COUNT ').

load_config(#{handler := _} = Env0) ->
  Env = maps:merge(#{listener_name => sqlapi_ranch}, Env0),

  MysqlPort = maps:get(port, Env, undefined),
  OldMysql = existing_port(Env),

  if
    OldMysql == MysqlPort -> ok;
    OldMysql == undefined andalso MysqlPort =/= undefined ->
      start_server(Env);
    MysqlPort =/= undefined andalso MysqlPort =/= OldMysql andalso OldMysql =/= undefined -> 
      stop_server(Env),
      start_server(Env);
    MysqlPort == undefined ->
      stop_server(Env)
  end.


existing_port(#{listener_name := Ref, trivial := true}) ->
  case whereis(Ref) of
    undefined -> undefined;
    Pid -> 
      case process_info(Pid, dictionary) of
        {dictionary, Dict} -> proplists:get_value('$my_listener_port',Dict);
        _ -> undefined
      end
  end;

existing_port(#{listener_name := Ref}) ->
  existing_port(Ref);

existing_port(Ref) when is_atom(Ref) ->
  Supervisors = supervisor:which_children(ranch_sup),
  case lists:keyfind({ranch_listener_sup,Ref}, 1, Supervisors) of
    {_, Pid, _, _} when is_pid(Pid) ->
      case process_info(Pid) of
        undefined -> undefined;
        _ -> ranch:get_port(Ref)
      end;
    _ -> 
      undefined
  end.

start_server(#{listener_name := Name, port := Port, handler := Handler, trivial := true} = Env) ->
  my_ranch_worker:start_trivial_server(Port, Name, Handler, Env);

start_server(#{listener_name := Name, port := HostPort, handler := Handler} = Env) ->
  {Host,Port} = if
    is_number(HostPort) -> {[], HostPort};
    is_binary(HostPort) ->
      [H,P] = binary:split(HostPort, <<":">>),
      {ok, Ip} = inet_parse:address(binary_to_list(H)),
      {[{ip,Ip}],binary_to_integer(P)}
  end,

  case gen_tcp:listen(Port, [binary, {reuseaddr, true}, {active, false}, {backlog, 4096}] ++ Host) of
    {ok, LSock} ->
      ranch:start_listener(Name, 5, ranch_tcp, [{socket, LSock},{max_connections,300}], my_ranch_worker, [Handler, Env]);
    {error, _}=Err -> 
      Err
  end.


stop_server(#{trivial := true, listener_name := Name}) ->
  case whereis(Name) of
    undefined -> ok;
    Pid -> erlang:exit(Pid, shutdown)
  end;

stop_server(#{listener_name := Name}) ->
  stop_server(Name);

stop_server(Name) when is_atom(Name) ->
  my_ranch_worker:stop_server(Name).



notify(Event, Metadata) ->
  case application:get_env(sqlapi, error_handler) of
    {ok, {M,F}} -> M:F(Event, Metadata);
    undefined -> error_logger:error_msg("~p: ~p",[Event,Metadata])
  end.




password_hash(PlaintextPassword, Salt) ->
  my_protocol:hash_password(PlaintextPassword, Salt).




create_temporary_table(TableName, TableSpec) ->
  Reply = sqlapi_ram_table:create_table(TableName, TableSpec),
  Reply.

drop_temporary_table(TableName) ->
  Reply = sqlapi_ram_table:drop_table(TableName),
  Reply.





%% filter, aggregate, order and limit rows after select query
apply_sql_filter(Rows, #sql_filter{conditions = Conditions} = Filter) when is_list(Rows) ->
  Rows1 = lists:map(fun
    (R) when is_list(R) -> R;
    (R) -> maps:to_list(R)
  end, Rows),
  Rows2 = [[{to_a(C), V} || {C, V} <- Row] || Row <- Rows1],
  Cond = mk_cond(Conditions),
  Rows3 = lists:foldl(fun
    (_S, {error,Err}) -> {error,Err};
    (S, Acc) ->
      case filter(Cond,S) of
        true  -> [S|Acc];
        false -> Acc;
        Err -> Err
      end
  end, [], Rows2),

  if not is_list(Rows3) -> 
      {error, 1036, <<"such filtering is not supported yet">>};
    true ->
      Rows4 = aggregate_by_sql(Rows3, Filter),
      Rows5 = order_by_sql(Rows4, Filter),
      Rows6 = limit_by_sql(Rows5, Filter),
      Rows7 = filter_columns_to_map(Rows6, Filter),
      Rows7
  end;
apply_sql_filter(Error, _Filter) when is_tuple(Error) ->
  Error.


aggregate_by_sql({error,_,_} = Err, _) ->
  Err;

aggregate_by_sql(Rows, #sql_filter{group = Group, columns = Columns}) ->
  Groupped = lists:foldl(fun(Row, Acc) ->
    Key = if Group == undefined -> undefined;
      true -> [proplists:lookup(C, Row) || C <- Group]
    end,
    RowsForKey = maps:get(Key, Acc, []),
    Acc#{Key => [Row| RowsForKey]}
  end, #{}, Rows),
  NeedCount = lists:member(?COUNT_COL_NAME, Columns),
  Rows1 = lists:flatmap(fun
    ({undefined, GroupRows}) when NeedCount -> add_count(GroupRows, length(GroupRows));
    ({undefined, GroupRows}) -> GroupRows;
    ({_, GroupRows}) when NeedCount -> add_count([hd(GroupRows)], length(GroupRows));
    ({_, GroupRows}) -> [hd(GroupRows)]
  end, maps:to_list(Groupped)),
  Rows1.


add_count([Row| Rows], Count) ->
  Row1 = [{?COUNT_COL_NAME, Count}| Row],
  [Row1| add_count(Rows, Count)];
add_count([], _) -> [].



order_by_sql(Rows, #sql_filter{order = Order, table_columns = TableColumns})
  when length(Rows) > 0, Order /= undefined ->
  Keys = proplists:get_keys(TableColumns),
  KeyOrder = [{to_a(Key), Sort} || #order{key = Key, sort = Sort} <- Order,
                                   lists:member(to_a(Key), Keys)],
  if length(KeyOrder) /= length(Order) -> {error, 1036, <<"unknown column in order clause">>};
    true ->
      Rows1 = lists:sort(fun(R1, R2) ->
        Pred = lists:foldl(fun
          ({Key, Sort}, undefined) ->
            Val1 = proplists:get_value(Key, R1, undefined),
            Val2 = proplists:get_value(Key, R2, undefined),
            case Sort of
              _ when Val1 == Val2 -> undefined;
              asc when Val1 == undefined -> true; % null first mysql default
              asc when Val2 == undefined -> false;
              asc -> Val1 < Val2;
              desc when Val1 == undefined -> false; % null last mysql default
              desc when Val2 == undefined -> true;
              desc ->  Val1 > Val2
            end;
          (_, Acc) -> Acc
        end, undefined, KeyOrder),
        Pred == undefined orelse Pred
      end, Rows),
      Rows1
  end;
order_by_sql(Rows, _Filter) ->
  Rows.



%% limit/offset rows, convert to maps
limit_by_sql(Rows, #sql_filter{limit = Limit, offset = Offset}) when
  is_list(Rows) andalso (Limit =/= undefined orelse Offset =/= undefined) ->

  case Limit of
    undefined -> 
      Rows;
    Limit ->
      case Offset of
        undefined -> lists:sublist(Rows, 1, Limit);
        Offset when Offset < length(Rows) -> lists:sublist(Rows, Offset+1, Limit);
        _ -> []
      end
  end;

limit_by_sql(Rows, _) ->
  Rows.


filter_columns_to_map(Rows, #sql_filter{columns = Columns}) when is_list(Rows) ->
  Rows1 = lists:map(fun
    (Row) when Columns == [] -> 
      maps:from_list(Row);
    (Row) ->
      Row1 = [{C, proplists:get_value(C, Row)} || C <- Columns],
      maps:from_list(Row1)
  end, Rows),
  Rows1;
filter_columns_to_map(Err, _) when is_tuple(Err) ->
  Err.



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Erlang-side condition transformation %%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
filter({nexo_or, Cond1, Cond2}, Item) -> filter(Cond1, Item) orelse filter(Cond2, Item);
filter({nexo_and,Cond1, Cond2}, Item) -> filter(Cond1, Item) andalso filter(Cond2, Item);
filter({eq,  Arg1, Arg2},  Item) -> arith(eq, Arg1(Item), Arg2(Item));
filter({neq, Arg1, Arg2},  Item) -> not arith(eq, Arg1(Item), Arg2(Item));
filter({lt,  Arg1, Arg2},  Item) -> arith(lt, Arg1(Item), Arg2(Item));
filter({gt,  Arg1, Arg2},  Item) -> arith(gt, Arg1(Item), Arg2(Item));
filter({lte, Arg1, Arg2},  Item) -> arith(lte, Arg1(Item), Arg2(Item));
filter({gte, Arg1, Arg2},  Item) -> arith(gte, Arg1(Item), Arg2(Item));
filter({is,  Arg1, Arg2},  Item) -> arith(is, Arg1(Item), Arg2(Item));
filter({is_not,  Arg1, Arg2},  Item) -> not arith(is, Arg1(Item), Arg2(Item));
filter({not_in,  Arg1, Arg2},  Item) -> not lists:member(Arg1(Item), Arg2(Item));
filter({in,  Arg1, Arg2},  Item) -> V = Arg1(Item), lists:any(fun(Test) -> arith(eq, V, Test) end, Arg2(Item));
filter(no_cond,_)                -> true;
filter({like, Arg1, Arg2}, Item)  -> like(Arg1(Item), Arg2);
filter({not_like, Arg1, Arg2}, Item)  -> not_like(Arg1(Item), Arg2);
filter(_,_)                      -> {error, not_supported}.


arith(Op, V1, V2) when Op == eq; Op == is ->
  IsFalse = fun(V) ->
    lists:member(V, [false, 0] ++ if Op == is -> [undefined, null]; true -> [] end)
  end,
  if V1 == true orelse V1 == 1 -> not IsFalse(V2);
     V1 == V2 -> true;
     true -> IsFalse(V1) andalso IsFalse(V2)
  end;
arith(Op, V1, V2) when is_number(V1), is_number(V2) ->
  case Op of
    lt -> V1 < V2;
    gt -> V1 > V2;
    lte -> V1 =< V2;
    gte -> V1 >= V2
  end;
arith(_Op, _V1, _V2) ->
  false.


like(Value, Pattern) when is_binary(Value), is_binary(Pattern) ->
  like0(unicode:characters_to_list(Pattern), unicode:characters_to_list(Value));
like(undefined, Pattern) -> like(<<>>, Pattern);
like(_,_) -> {error, not_supported}.

not_like(Value, Pattern) ->
  case like(Value, Pattern) of
    true -> false;
    false -> true;
    Error -> Error
  end.

like0(P, V) when P == V -> true;

%% check \_, \%, \\
like0([$\\, $_ | P], [$_| V]) -> like0(P, V);
like0([$\\, $_ | _], _) -> false;
like0([$\\, $% | P], [$%| V]) -> like0(P, V);
like0([$\\, $% | _], _) -> false;
like0([$\\, $\\ | P], [$\\| V]) -> like0(P, V);
like0([$\\, $\\ | _], _) -> false;

%% '_': skip symbol
like0([$_| P], [_| V]) -> like0(P, V);

%% '%': skip symbols, check further pattern
like0([$%| []], _) -> true;
like0([$%| P], V) ->
  case like0(P, V) of
    false when V == [] -> false;
    false -> like0([$%| P], tl(V)); % keep pattern and continue until end of the string
    true -> true
  end;

like0([S| P], [S| V]) -> like0(P, V);

like0(_, _) -> false.


mk_cond(#condition{nexo=Op, op1=Arg1, op2=Arg2}) -> {Op, mk_cond(Arg1), mk_cond(Arg2)};
mk_cond(undefined)        -> no_cond;
mk_cond(#key{name = <<"false">>, table=undefined}) -> fun(_) -> false end;
mk_cond(#key{name =  <<"true">>, table=undefined}) -> fun(_) -> true end;
mk_cond(#key{name=Field})  -> fun
  (Item) when is_list(Item) -> proplists:get_value(binary_to_existing_atom(Field,utf8), Item);
  (Item) when is_map(Item) -> maps:get(binary_to_existing_atom(Field,utf8), Item, undefined) 
  end;
mk_cond(#value{value=Value})  -> fun(_) -> Value end;
mk_cond(#subquery{subquery=Set}) -> fun(_) -> Set end;
mk_cond(Arg)                     -> Arg.

% unpack_filter_conditions({condition, nexo_and, Cond1, Cond2}) ->
%   unpack_filter_conditions(Cond1) ++ unpack_filter_conditions(Cond2);

% unpack_filter_conditions({condition, Op, {key, Key, _, _}, {value, _, Value}}) when 
%   Op == eq; Op == gt; Op == gte; Op == lt; Op == lte ->
%   [{{Key,Op},Value}].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Extraction single condition from tree %%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
find_constraint([], _) -> not_found;
find_constraint([C|Rest], Cond) -> 
  case find_constraint(C, Cond) of
    not_found -> find_constraint(Rest, Cond);
    Value -> Value
  end;

%% single constraint search
find_constraint({Op, Key}, {condition, Op, Value, #key{name=Key}}) -> Value;
find_constraint({Op, Key}, {condition, Op, #key{name=Key}, Value}) -> Value;
find_constraint(Part, {condition, _, Arg1, Arg2}) -> 
  case find_constraint(Part, Arg1) of
    not_found -> find_constraint(Part, Arg2);
    Value -> Value
  end;

find_constraint(_,_) -> 
  not_found.
                  

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% SQLite condition transformation %%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
sqlite_flt(undefined) -> {"",[]};
sqlite_flt(Cond) -> 
  {C, Par} = sqlite_flt(Cond, []),
  {" WHERE " ++ C, Par}.

sqlite_flt({key,Arg,_,_},Par)    -> {binary_to_existing_atom(Arg,utf8), Par};
sqlite_flt({value,_,Arg},Par)    -> {"?", [Arg|Par]};
sqlite_flt({subquery,_,Set},Par) -> {["(?", [",?"||_<-tl(Set)], ")"], [Set|Par]};
sqlite_flt({condition, Op,  Arg1, Arg2},  Par) -> 
  %% reverse argument traverse order to maintain resulting parameter order
  {A2, P2} = sqlite_flt(Arg2,Par),
  {A1, P1} = sqlite_flt(Arg1,P2),
  Query = lists:flatten(io_lib:format("(~s ~s ~s)", [A1,sqlite_op(Op),A2])),
  {Query,P1}.

sqlite_op(nexo_and) -> "and";
sqlite_op(nexo_or)  -> "or";
sqlite_op(eq)  -> "=";
sqlite_op(lt)  -> "<";
sqlite_op(gt)  -> ">";
sqlite_op(lte) -> "<=";
sqlite_op(gte) -> ">=";   
sqlite_op(in)  -> "in".






% $$\                          $$\           
% $$ |                         \__|          
% $$ |      $$$$$$\   $$$$$$\  $$\  $$$$$$$\ 
% $$ |     $$  __$$\ $$  __$$\ $$ |$$  _____|
% $$ |     $$ /  $$ |$$ /  $$ |$$ |$$ /      
% $$ |     $$ |  $$ |$$ |  $$ |$$ |$$ |      
% $$$$$$$$\\$$$$$$  |\$$$$$$$ |$$ |\$$$$$$$\ 
% \________|\______/  \____$$ |\__| \_______|
%                    $$\   $$ |              
%                    \$$$$$$  |              
%                     \______/               


execute(#request{text = Text} = Request, Handler, State) ->
  try execute0(Request, Handler, State) of
    {reply, #response{}, _} = Reply ->
      Reply
  catch
    _C:E ->
      ST = erlang:get_stacktrace(),
      notify(sql_handler_error,[{module,?MODULE},{line,?LINE},{pid,self()},{application,sqlapi},
        {query, Text}, {error, E}, {stacktrace, iolist_to_binary(io_lib:format("~p",[ST]))}]),
      {reply, #response{status=?STATUS_ERR, error_code=500, info= <<"Internal server error">>}, State}
  end.



execute0(#request{info = #select{tables = [#table{name = Table}]} = Select}, Handler, State) when is_binary(Table) ->

  case Handler:columns(Table, State) of
    {error, no_table} ->
      {reply, #response{status=?STATUS_ERR, error_code=1146, info = <<"Table '",Table/binary,"' doesn't exist">>}, State};

    TableColumns when is_list(TableColumns) ->
      Filter = build_sql_filter(Select, TableColumns),
      Columns = Filter#sql_filter.columns,
      case Handler:select(Table, Filter, State) of 
        {error, Code, Desc} -> 
          {reply, #response{status=?STATUS_ERR, error_code=Code, info=Desc}, State};

        Rows when is_list(Rows) ->
          ResponseColumns = [case lists:keyfind(Name,1,TableColumns) of
            _ when Name == ?COUNT_COL_NAME -> {<<"COUNT">>, integer};
            false -> {Name, string};
            Col -> Col
          end || Name <- Columns],
          Response = { response_columns(ResponseColumns), [response_row(Row, Columns) || Row <- Rows]},
          Reply = #response{status=?STATUS_OK, info = Response},
          {reply, Reply, State}
      end
  end;


execute0(#request{info = #insert{table = #table{name = Table}, values = ValuesSpec}}, Handler, State) ->
  Values = lists:map(fun(Row) -> 
    maps:from_list(lists:map(fun
      (#set{key = K, value = #value{value = V}}) -> {K,V};
      (#set{key = K, value = #key{name = <<"true">>}}) -> {K,true};
      (#set{key = K, value = #key{name = <<"TRUE">>}}) -> {K,true};
      (#set{key = K, value = #key{name = <<"false">>}}) -> {K,false};
      (#set{key = K, value = #key{name = <<"FALSE">>}}) -> {K,false}
    end, Row))
  end, ValuesSpec),
  case Handler:insert(Table, Values, State) of
    {error, Code, Desc} ->
      {reply, #response{status=?STATUS_ERR, error_code=Code, info=Desc}, State};
    {ok, #{status := Status} = Reply} ->
      StatusCode = case Status of
        ok -> ?STATUS_OK
      end,
      AffectedRows = maps:get(affected_rows, Reply, 1),
      Id = maps:get(id, Reply, 0),
      Info = maps:get(info, Reply, <<>>),
      Warnings = maps:get(warnings, Reply, 0),
      {reply, #response{status=StatusCode, affected_rows = AffectedRows, last_insert_id = Id, status_flags = 0, warnings = Warnings, info = Info}, State};
    {ok, Id} when is_integer(Id) ->
      {reply, #response{status=?STATUS_OK, affected_rows = 1, last_insert_id = Id, status_flags = 0, warnings = 0, info = <<>>}, State}
  end;



execute0(#request{info= #update{table = #table{name=Table}, set=ValuesSpec, conditions=Conditions}}, Handler, State) ->
  Values = maps:from_list([{K,V} || #set{key = K, value = #value{value = V}} <- ValuesSpec]),
  case Handler:update(Table, Values, Conditions, State) of
    {error, Code, Desc} ->
      {reply, #response{status=?STATUS_ERR, error_code=Code, info=Desc}, State};
    {ok, #{status := Status} = Reply} ->
      StatusCode = case Status of
        ok -> ?STATUS_OK
      end,
      AffectedRows = maps:get(affected_rows, Reply, 1),
      Id = maps:get(id, Reply, 0),
      Info = maps:get(info, Reply, <<>>),
      Warnings = maps:get(warnings, Reply, 0),
      {reply, #response{status=StatusCode, affected_rows = AffectedRows, last_insert_id = Id, status_flags = 0, warnings = Warnings, info = Info}, State};
    {ok, Id} when is_integer(Id) ->
      {reply, #response{status=?STATUS_OK, affected_rows = 1, last_insert_id = Id, status_flags = 0, warnings = 0, info = <<>>}, State}
  end;



execute0(#request{info = #delete{table = #table{name = Table}, conditions = Conditions}}, Handler, State) ->
  case Handler:delete(Table, Conditions, State) of
    {error, Code, Desc} ->
      {reply, #response{status=?STATUS_ERR, error_code=Code, info=Desc}, State};
    {ok, #{status := Status} = Reply} ->
      StatusCode = case Status of
        ok -> ?STATUS_OK
      end,
      AffectedRows = maps:get(affected_rows, Reply, 1),
      Id = maps:get(id, Reply, 0),
      Info = maps:get(info, Reply, <<>>),
      Warnings = maps:get(warnings, Reply, 0),
      {reply, #response{status=StatusCode, affected_rows = AffectedRows, last_insert_id = Id, status_flags = 0, warnings = Warnings, info = Info}, State};
    {ok, Id} when is_integer(Id) ->
      {reply, #response{status=?STATUS_OK, affected_rows = 1, last_insert_id = Id, status_flags = 0, warnings = 0, info = <<>>}, State}
  end;

execute0(#request{info = #select{params = [#function{name = Name, params = Params0}]}}, Handler, State) ->
  Params = [V || #value{value=V} <- Params0],
  case Handler:fncall(Name, Params, State) of
    {error, Code, Desc} ->
      {reply, #response{status=?STATUS_ERR, error_code=Code, info=Desc}, State};
    {ok, Columns, Rows} ->
      Columns1 = [C || {C,_} <- Columns],
      Response = {response_columns(Columns), [response_row(Row, Columns1) || Row <- Rows]},
      % Response = {response_columns(Columns), Rows},
      {reply, #response{status=?STATUS_OK, info = Response}, State};
    {ok, #{status := Status} = Reply} ->
      StatusCode = case Status of
        ok -> ?STATUS_OK
      end,
      AffectedRows = maps:get(affected_rows, Reply, 1),
      Id = maps:get(id, Reply, 0),
      Info = maps:get(info, Reply, <<>>),
      Warnings = maps:get(warnings, Reply, 0),
      {reply, #response{status=StatusCode, affected_rows = AffectedRows, last_insert_id = Id, status_flags = 0, warnings = Warnings, info = Info}, State};
    {ok, Id} when is_integer(Id) ->
      {reply, #response{status=?STATUS_OK, affected_rows = 1, last_insert_id = Id, status_flags = 0, warnings = 0, info = <<>>}, State};
    ok -> 
      {reply, #response{status=?STATUS_OK}, State}
  end;

execute0(#request{text = Text, info = {error,{_,sql92_parser,Desc}}}, _Handler, State) ->
  Description = iolist_to_binary(["Invalid query: ",Desc]),
  notify(sql_error,[{module,?MODULE},{line,?LINE},{pid,self()},{application,sqlapi},{query, Text}, {error, bad_query}]),
  {reply, #response{status=?STATUS_ERR, error_code=1065, info= Description}, State};

execute0(#request{text = Text}, _Handler, State) ->
  notify(sql_error,[{module,?MODULE},{line,?LINE},{pid,self()},{application,sqlapi},{query, Text}, {error, unknown_query}]),
  {reply, #response{status=?STATUS_ERR, error_code=500, info= <<"Internal server error">>}, State}.







build_sql_filter(#select{params = Params, conditions = Conditions0, order = Order, group = Group0, 
  limit = Limit, offset = Offset}, TableColumns) when is_list(TableColumns) ->

  Conditions = normalize_condition_types(TableColumns, Conditions0),

  Columns = lists:flatmap(fun
    (#all{}) -> [N || {N,_} <- TableColumns];
    (#key{name = Name}) -> [binary_to_existing_atom(Name,latin1)];
    (#function{name = N}) when N == <<"COUNT">> orelse N == <<"count">> -> [?COUNT_COL_NAME];
    (_) -> [] %  other aggregate, etc
  end, Params),

  Group = if Group0 == undefined -> undefined;
    true -> [binary_to_existing_atom(C,latin1) || C <- Group0]
  end,

  #sql_filter{
     conditions = Conditions,
     columns = Columns,
     table_columns = TableColumns,
     order = Order,
     group = Group,
     limit = Limit,
     offset = Offset}.


normalize_condition_types(Columns, #condition{nexo = OrAnd, op1 = Op1, op2 = Op2}) when OrAnd == nexo_and; OrAnd == nexo_or ->
  #condition{nexo = OrAnd, 
    op1 = normalize_condition_types(Columns, Op1),
    op2 = normalize_condition_types(Columns, Op2)
  };

normalize_condition_types(Columns, #condition{nexo = C, op1 = #key{name = Name} = K, op2 = #key{name = Bool}} = Cond) 
  when Bool == <<"true">>; Bool == <<"false">>; Bool == <<"TRUE">>; Bool == <<"FALSE">> ->
  Column = binary_to_existing_atom(Name,latin1),
  case proplists:get_value(Column, Columns) of
    boolean when Bool == <<"true">>; Bool == <<"TRUE">> -> #condition{nexo = C, op1 = K, op2 = #value{value = true}};
    boolean when Bool == <<"FALSE">>; Bool == <<"FALSE">> -> #condition{nexo = C, op1 = K, op2 = #value{value = false}};
    _ -> Cond
  end;


normalize_condition_types(Columns, #condition{nexo = C, op1 = #key{name = Name} = K, op2 = #value{value = V}} = Cond) when V == 1; V == 0->
  Column = binary_to_existing_atom(Name,latin1),
  case proplists:get_value(Column, Columns) of
    boolean when V == 1 -> #condition{nexo = C, op1 = K, op2 = #value{value = true}};
    boolean when V == 0 -> #condition{nexo = C, op1 = K, op2 = #value{value = false}};
    _ -> Cond
  end;

normalize_condition_types(_Columns, Cond) ->
  Cond.




response_row(Row, Columns) when is_map(Row) ->
  [maps:get(Column, Row, undefined) || Column <- Columns];

response_row(Row, Columns) when is_list(Row) ->
  [proplists:get_value(Column, Row) || Column <- Columns].


response_columns(Columns) ->
  lists:map(fun
    ({Name,string}) -> #column{name = to_b(Name), type = ?TYPE_VAR_STRING, length = 20, org_name = to_b(Name)};
    ({Name,boolean}) -> #column{name = to_b(Name), type = ?TYPE_TINY, length = 1, org_name = to_b(Name)};
    ({Name,integer}) -> #column{name = to_b(Name), type = ?TYPE_LONGLONG, length = 20, org_name = to_b(Name)}
  end, Columns).

to_b(Atom) when is_atom(Atom) -> atom_to_binary(Atom,latin1);
to_b(Bin) when is_binary(Bin) -> Bin.

to_a(Atom) when is_binary(Atom) -> binary_to_atom(Atom,latin1);
to_a(Atom) when is_atom(Atom) -> Atom.




terminate(_Reason,_) -> 
  ok.
