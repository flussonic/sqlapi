-module(sql92_parser).
-export([parse/1, parse_and_scan/1, format_error/1]).
-file("src/sql92_parser.yrl", 297).
-include("../include/sql.hrl").
-export([compare/1]).

value_of(Token) -> element(3, Token).
map_key(Key, #{}=KV) -> maps:get(Key, KV, undefined);
map_key(_, _) -> undefined.

tolower(Bin) when is_binary(Bin) -> list_to_binary(string:to_lower(binary_to_list(Bin))).

apply_alias(V, undefined) -> V;
apply_alias(#function{}=V, A) -> V#function{alias=A};
apply_alias(#subquery{}=V, A) -> V#subquery{name=A};
apply_alias(#value{}=V, A) -> V#value{name=A};
apply_alias(#key{}=V, A) -> V#key{alias=A};
apply_alias(V, _) -> V.

zip_insert_values(undefined, Rows) -> Rows;
zip_insert_values(Keys, Rows) -> 
	[ [#set{key=Key, value=Val} 
		|| {Key, Val} <- lists:zip(Keys, Row)] 
	|| Row <- Rows].


compare(Q) ->
	{T1,N} = {0, undefined}, % timer:tc(fun() -> mysql_proto_old:parse(Q) end),
	{T3,{T2, P}} = timer:tc(fun() -> 
		case timer:tc(fun() -> sql92_scan:string(Q) end) of
			{T2_, {ok, T, _}} -> 
				% ct:pal("SCAN ~p", [T]),
				case sql92_parser:parse(T) of
					{ok, [P_]} -> {T2_, P_};
					PE_ -> {parse_error, PE_}
				end;
			LE_ -> {lex_error, LE_}
		end
	end),

	{true, {T1, vs, T2, T3}, N, P}.
-file("/usr/local/Cellar/erlang/20.0/lib/erlang/lib/parsetools-2.1.5/include/yeccpre.hrl", 0).
%%
%% %CopyrightBegin%
%%
%% Copyright Ericsson AB 1996-2017. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%
%% %CopyrightEnd%
%%

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% The parser generator will insert appropriate declarations before this line.%

-type yecc_ret() :: {'error', _} | {'ok', _}.

-spec parse(Tokens :: list()) -> yecc_ret().
parse(Tokens) ->
    yeccpars0(Tokens, {no_func, no_line}, 0, [], []).

-spec parse_and_scan({function() | {atom(), atom()}, [_]}
                     | {atom(), atom(), [_]}) -> yecc_ret().
parse_and_scan({F, A}) ->
    yeccpars0([], {{F, A}, no_line}, 0, [], []);
parse_and_scan({M, F, A}) ->
    Arity = length(A),
    yeccpars0([], {{fun M:F/Arity, A}, no_line}, 0, [], []).

-spec format_error(any()) -> [char() | list()].
format_error(Message) ->
    case io_lib:deep_char_list(Message) of
        true ->
            Message;
        _ ->
            io_lib:write(Message)
    end.

%% To be used in grammar files to throw an error message to the parser
%% toplevel. Doesn't have to be exported!
-compile({nowarn_unused_function, return_error/2}).
-spec return_error(integer(), any()) -> no_return().
return_error(Line, Message) ->
    throw({error, {Line, ?MODULE, Message}}).

-define(CODE_VERSION, "1.4").

yeccpars0(Tokens, Tzr, State, States, Vstack) ->
    try yeccpars1(Tokens, Tzr, State, States, Vstack)
    catch 
        error: Error ->
            Stacktrace = erlang:get_stacktrace(),
            try yecc_error_type(Error, Stacktrace) of
                Desc ->
                    erlang:raise(error, {yecc_bug, ?CODE_VERSION, Desc},
                                 Stacktrace)
            catch _:_ -> erlang:raise(error, Error, Stacktrace)
            end;
        %% Probably thrown from return_error/2:
        throw: {error, {_Line, ?MODULE, _M}} = Error ->
            Error
    end.

yecc_error_type(function_clause, [{?MODULE,F,ArityOrArgs,_} | _]) ->
    case atom_to_list(F) of
        "yeccgoto_" ++ SymbolL ->
            {ok,[{atom,_,Symbol}],_} = erl_scan:string(SymbolL),
            State = case ArityOrArgs of
                        [S,_,_,_,_,_,_] -> S;
                        _ -> state_is_unknown
                    end,
            {Symbol, State, missing_in_goto_table}
    end.

yeccpars1([Token | Tokens], Tzr, State, States, Vstack) ->
    yeccpars2(State, element(1, Token), States, Vstack, Token, Tokens, Tzr);
yeccpars1([], {{F, A},_Line}, State, States, Vstack) ->
    case apply(F, A) of
        {ok, Tokens, Endline} ->
            yeccpars1(Tokens, {{F, A}, Endline}, State, States, Vstack);
        {eof, Endline} ->
            yeccpars1([], {no_func, Endline}, State, States, Vstack);
        {error, Descriptor, _Endline} ->
            {error, Descriptor}
    end;
yeccpars1([], {no_func, no_line}, State, States, Vstack) ->
    Line = 999999,
    yeccpars2(State, '$end', States, Vstack, yecc_end(Line), [],
              {no_func, Line});
yeccpars1([], {no_func, Endline}, State, States, Vstack) ->
    yeccpars2(State, '$end', States, Vstack, yecc_end(Endline), [],
              {no_func, Endline}).

%% yeccpars1/7 is called from generated code.
%%
%% When using the {includefile, Includefile} option, make sure that
%% yeccpars1/7 can be found by parsing the file without following
%% include directives. yecc will otherwise assume that an old
%% yeccpre.hrl is included (one which defines yeccpars1/5).
yeccpars1(State1, State, States, Vstack, Token0, [Token | Tokens], Tzr) ->
    yeccpars2(State, element(1, Token), [State1 | States],
              [Token0 | Vstack], Token, Tokens, Tzr);
yeccpars1(State1, State, States, Vstack, Token0, [], {{_F,_A}, _Line}=Tzr) ->
    yeccpars1([], Tzr, State, [State1 | States], [Token0 | Vstack]);
yeccpars1(State1, State, States, Vstack, Token0, [], {no_func, no_line}) ->
    Line = yecctoken_end_location(Token0),
    yeccpars2(State, '$end', [State1 | States], [Token0 | Vstack],
              yecc_end(Line), [], {no_func, Line});
yeccpars1(State1, State, States, Vstack, Token0, [], {no_func, Line}) ->
    yeccpars2(State, '$end', [State1 | States], [Token0 | Vstack],
              yecc_end(Line), [], {no_func, Line}).

%% For internal use only.
yecc_end({Line,_Column}) ->
    {'$end', Line};
yecc_end(Line) ->
    {'$end', Line}.

yecctoken_end_location(Token) ->
    try erl_anno:end_location(element(2, Token)) of
        undefined -> yecctoken_location(Token);
        Loc -> Loc
    catch _:_ -> yecctoken_location(Token)
    end.

-compile({nowarn_unused_function, yeccerror/1}).
yeccerror(Token) ->
    Text = yecctoken_to_string(Token),
    Location = yecctoken_location(Token),
    {error, {Location, ?MODULE, ["syntax error before: ", Text]}}.

-compile({nowarn_unused_function, yecctoken_to_string/1}).
yecctoken_to_string(Token) ->
    try erl_scan:text(Token) of
        undefined -> yecctoken2string(Token);
        Txt -> Txt
    catch _:_ -> yecctoken2string(Token)
    end.

yecctoken_location(Token) ->
    try erl_scan:location(Token)
    catch _:_ -> element(2, Token)
    end.

-compile({nowarn_unused_function, yecctoken2string/1}).
yecctoken2string({atom, _, A}) -> io_lib:write_atom(A);
yecctoken2string({integer,_,N}) -> io_lib:write(N);
yecctoken2string({float,_,F}) -> io_lib:write(F);
yecctoken2string({char,_,C}) -> io_lib:write_char(C);
yecctoken2string({var,_,V}) -> io_lib:format("~s", [V]);
yecctoken2string({string,_,S}) -> io_lib:write_string(S);
yecctoken2string({reserved_symbol, _, A}) -> io_lib:write(A);
yecctoken2string({_Cat, _, Val}) -> io_lib:format("~tp", [Val]);
yecctoken2string({dot, _}) -> "'.'";
yecctoken2string({'$end', _}) -> [];
yecctoken2string({Other, _}) when is_atom(Other) ->
    io_lib:write_atom(Other);
yecctoken2string(Other) ->
    io_lib:format("~tp", [Other]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%



-file("src/sql92_parser.erl", 215).

-dialyzer({nowarn_function, yeccpars2/7}).
yeccpars2(0=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_0(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(1=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_1(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(2=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_2(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(3=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_3(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(4=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_4(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(5=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_5(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(6=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_6(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(7=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_7(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(8=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_8(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(9=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_9(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(10=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_10(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(11=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_11(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(12=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_12(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(13=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_13(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(14=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_14(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(15=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_15(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(16=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_16(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(17=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_17(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(18=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_18(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(19=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_19(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(20=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_20(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(21=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_21(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(22=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_16(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(23=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_23(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(24=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_24(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(25=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_25(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(26=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_26(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(27=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_27(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(28=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_28(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(29=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_29(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(30=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_30(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(31=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_31(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(32=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_32(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(33=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_33(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(34=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_34(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(35=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_35(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(36=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_36(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(37=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_37(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(38=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_38(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(39=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_39(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(40=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_40(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(41=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_41(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(42=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_42(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(43=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_43(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(44=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_44(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(45=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_45(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(46=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_46(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(47=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_39(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(48=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_48(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(49=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_49(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(50=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_50(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(51=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_51(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(52=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_52(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(53=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_53(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(54=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_54(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(55=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_55(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(56=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_56(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(57=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_57(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(58=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_58(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(59=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_59(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(60=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_60(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(61=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_39(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(62=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_39(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(63=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_63(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(64=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_39(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(65=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_39(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(66=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_66(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(67=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_67(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(68=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_68(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(69=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_69(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(70=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_70(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(71=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_71(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(72=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_39(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(73=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_73(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(74=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_74(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(75=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_75(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(76=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_76(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(77=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_77(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(78=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_78(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(79=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_79(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(80=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_80(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(81=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_81(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(82=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_82(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(83=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_83(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(84=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_84(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(85=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_85(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(86=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_86(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(87=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_87(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(88=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_31(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(89=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_89(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(90=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_90(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(91=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_91(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(92=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_92(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(93=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_93(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(94=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_94(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(95=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_95(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(96=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_96(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(97=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_97(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(98=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_98(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(99=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_99(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(100=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_100(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(101=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_101(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(102=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_92(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(103=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_92(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(104=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_104(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(105=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_92(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(106=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_92(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(107=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_107(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(108=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_108(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(109=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_109(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(110=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_110(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(111=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_39(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(112=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_39(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(113=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_39(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(114=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_39(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(115=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_39(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(116=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_39(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(117=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_39(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(118=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_39(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(119=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_39(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(120=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_39(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(121=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_121(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(122=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_122(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(123=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_123(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(124=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_39(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(125=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_125(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(126=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_122(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(127=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_127(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(128=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_128(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(129=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_129(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(130=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_130(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(131=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_131(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(132=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_122(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(133=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_133(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(134=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_134(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(135=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_135(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(136=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_136(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(137=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_39(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(138=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_138(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(139=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_139(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(140=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_128(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(141=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_141(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(142=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_142(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(143=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_143(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(144=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_144(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(145=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_145(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(146=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_39(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(147=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_147(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(148=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_148(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(149=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_149(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(150=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_150(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(151=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_151(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(152=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_152(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(153=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_153(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(154=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_154(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(155=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_155(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(156=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_156(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(157=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_157(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(158=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_158(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(159=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_159(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(160=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_39(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(161=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_161(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(162=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_39(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(163=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_163(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(164=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_164(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(165=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_165(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(166=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_166(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(167=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_167(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(168=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_168(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(169=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_169(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(170=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_170(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(171=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_171(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(172=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_172(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(173=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_173(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(174=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_174(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(175=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_175(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(176=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_176(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(177=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_177(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(178=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_178(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(179=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_179(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(180=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_180(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(181=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_181(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(182=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_182(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(183=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_183(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(184=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_122(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(185=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_185(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(186=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_186(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(187=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_187(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(188=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_188(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(189=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_189(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(190=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_190(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(191=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_20(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(192=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_192(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(193=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_193(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(194=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_194(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(195=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_39(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(196=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_196(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(197=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_197(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(198=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_198(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(199=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_199(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(200=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_200(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(201=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_201(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(202=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_202(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(203=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_203(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(204=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_204(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(205=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_205(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(206=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_206(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(207=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_207(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(208=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_208(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(209=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_209(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(210=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_19(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(211=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_211(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(212=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_212(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(213=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_213(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(214=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_214(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(215=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_122(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(216=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_122(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(217=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_217(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(218=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_218(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(219=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_122(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(220=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_122(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(221=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_221(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(222=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_222(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(223=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_223(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(224=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_224(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(225=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_225(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(226=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_226(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(227=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_227(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(228=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_228(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(229=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_229(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(230=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_230(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(231=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_231(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(232=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_214(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(233=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_233(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(234=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_234(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(235=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_235(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(236=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_236(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(237=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_237(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(238=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_238(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(239=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_239(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(240=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_240(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(241=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_241(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(242=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_242(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(243=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_243(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(244=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_244(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(245=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_245(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(246=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_246(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(247=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_247(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(248=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_248(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(249=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_238(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(250=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_250(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(251=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_251(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(252=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_252(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(253=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_253(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(254=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_254(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(255=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_255(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(256=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_256(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(257=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_253(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(258=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_258(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(259=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_253(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(260=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_260(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(261=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_261(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(262=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_92(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(263=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_263(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(264=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_264(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(265=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_16(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(266=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_266(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(267=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_267(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(268=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_268(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(269=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_269(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(270=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_36(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(271=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_31(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(272=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_272(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(273=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_273(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(274=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_31(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(275=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_275(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(276=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_276(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(277=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_277(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(278=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_36(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(279=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_279(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(280=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_280(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(281=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_281(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(282=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_282(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(283=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_283(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(284=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_284(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(285=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_39(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(286=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_286(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(287=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_287(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(288=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_39(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(289=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_289(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(290=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_290(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(291=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_282(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(292=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_292(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(293=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_293(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(294=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_16(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(295=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_295(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(296=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_296(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(297=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_297(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(298=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_298(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(Other, _, _, _, _, _, _) ->
 erlang:error({yecc_bug,"1.4",{missing_state_in_action_table, Other}}).

-dialyzer({nowarn_function, yeccpars2_0/7}).
yeccpars2_0(S, 'begin', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 13, Ss, Stack, T, Ts, Tzr);
yeccpars2_0(S, commit, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 14, Ss, Stack, T, Ts, Tzr);
yeccpars2_0(S, delete, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 15, Ss, Stack, T, Ts, Tzr);
yeccpars2_0(S, describe, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 16, Ss, Stack, T, Ts, Tzr);
yeccpars2_0(S, insert, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 17, Ss, Stack, T, Ts, Tzr);
yeccpars2_0(S, rollback, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 18, Ss, Stack, T, Ts, Tzr);
yeccpars2_0(S, select, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 19, Ss, Stack, T, Ts, Tzr);
yeccpars2_0(S, set, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 20, Ss, Stack, T, Ts, Tzr);
yeccpars2_0(S, show, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 21, Ss, Stack, T, Ts, Tzr);
yeccpars2_0(S, update, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 22, Ss, Stack, T, Ts, Tzr);
yeccpars2_0(S, use, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 23, Ss, Stack, T, Ts, Tzr);
yeccpars2_0(_, _, _, _, T, _, _) ->
 yeccerror(T).

yeccpars2_1(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccgoto_manipulative_stmt(hd(Ss), Cat, Ss, Stack, T, Ts, Tzr).

yeccpars2_2(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccgoto_manipulative_stmt(hd(Ss), Cat, Ss, Stack, T, Ts, Tzr).

yeccpars2_3(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccgoto_manipulative_stmt(hd(Ss), Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccpars2_4/7}).
yeccpars2_4(_S, '$end', _Ss, Stack, _T, _Ts, _Tzr) ->
 {ok, hd(Stack)};
yeccpars2_4(_, _, _, _, T, _, _) ->
 yeccerror(T).

yeccpars2_5(S, ';', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 297, Ss, Stack, T, Ts, Tzr);
yeccpars2_5(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 NewStack = yeccpars2_5_(Stack),
 yeccgoto_sql_list(hd(Ss), Cat, Ss, NewStack, T, Ts, Tzr).

yeccpars2_6(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccgoto_manipulative_stmt(hd(Ss), Cat, Ss, Stack, T, Ts, Tzr).

yeccpars2_7(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccgoto_manipulative_stmt(hd(Ss), Cat, Ss, Stack, T, Ts, Tzr).

yeccpars2_8(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccgoto_manipulative_stmt(hd(Ss), Cat, Ss, Stack, T, Ts, Tzr).

yeccpars2_9(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccgoto_sql(hd(Ss), Cat, Ss, Stack, T, Ts, Tzr).

yeccpars2_10(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccgoto_manipulative_stmt(hd(Ss), Cat, Ss, Stack, T, Ts, Tzr).

yeccpars2_11(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccgoto_manipulative_stmt(hd(Ss), Cat, Ss, Stack, T, Ts, Tzr).

yeccpars2_12(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccgoto_manipulative_stmt(hd(Ss), Cat, Ss, Stack, T, Ts, Tzr).

yeccpars2_13(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 NewStack = yeccpars2_13_(Stack),
 yeccgoto_transaction_stmt(hd(Ss), Cat, Ss, NewStack, T, Ts, Tzr).

yeccpars2_14(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 NewStack = yeccpars2_14_(Stack),
 yeccgoto_transaction_stmt(hd(Ss), Cat, Ss, NewStack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccpars2_15/7}).
yeccpars2_15(S, from, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 294, Ss, Stack, T, Ts, Tzr);
yeccpars2_15(_, _, _, _, T, _, _) ->
 yeccerror(T).

-dialyzer({nowarn_function, yeccpars2_16/7}).
yeccpars2_16(S, name, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 27, Ss, Stack, T, Ts, Tzr);
yeccpars2_16(_, _, _, _, T, _, _) ->
 yeccerror(T).

-dialyzer({nowarn_function, yeccpars2_17/7}).
yeccpars2_17(S, into, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 265, Ss, Stack, T, Ts, Tzr);
yeccpars2_17(_, _, _, _, T, _, _) ->
 yeccerror(T).

yeccpars2_18(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 NewStack = yeccpars2_18_(Stack),
 yeccgoto_transaction_stmt(hd(Ss), Cat, Ss, NewStack, T, Ts, Tzr).

yeccpars2_19(S, '(', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 202, Ss, Stack, T, Ts, Tzr);
yeccpars2_19(S, '*', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 48, Ss, Stack, T, Ts, Tzr);
yeccpars2_19(S, cast, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 49, Ss, Stack, T, Ts, Tzr);
yeccpars2_19(S, name, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 51, Ss, Stack, T, Ts, Tzr);
yeccpars2_19(S, null, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 52, Ss, Stack, T, Ts, Tzr);
yeccpars2_19(S, var, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 54, Ss, Stack, T, Ts, Tzr);
yeccpars2_19(S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_122(S, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccpars2_20/7}).
yeccpars2_20(S, name, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 190, Ss, Stack, T, Ts, Tzr);
yeccpars2_20(S, var, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 54, Ss, Stack, T, Ts, Tzr);
yeccpars2_20(_, _, _, _, T, _, _) ->
 yeccerror(T).

yeccpars2_21(S, full, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 168, Ss, Stack, T, Ts, Tzr);
yeccpars2_21(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 NewStack = yeccpars2_21_(Stack),
 yeccpars2_167(167, Cat, [21 | Ss], NewStack, T, Ts, Tzr).

%% yeccpars2_22: see yeccpars2_16

-dialyzer({nowarn_function, yeccpars2_23/7}).
yeccpars2_23(S, name, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 24, Ss, Stack, T, Ts, Tzr);
yeccpars2_23(_, _, _, _, T, _, _) ->
 yeccerror(T).

yeccpars2_24(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_|Nss] = Ss,
 NewStack = yeccpars2_24_(Stack),
 yeccgoto_use_stmt(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_25(S, as, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 164, Ss, Stack, T, Ts, Tzr);
yeccpars2_25(S, name, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 165, Ss, Stack, T, Ts, Tzr);
yeccpars2_25(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 NewStack = yeccpars2_25_(Stack),
 yeccgoto_table_name(hd(Ss), Cat, Ss, NewStack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccpars2_26/7}).
yeccpars2_26(S, set, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 31, Ss, Stack, T, Ts, Tzr);
yeccpars2_26(_, _, _, _, T, _, _) ->
 yeccerror(T).

yeccpars2_27(S, '.', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 28, Ss, Stack, T, Ts, Tzr);
yeccpars2_27(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 NewStack = yeccpars2_27_(Stack),
 yeccgoto_tname(hd(Ss), Cat, Ss, NewStack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccpars2_28/7}).
yeccpars2_28(S, name, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 29, Ss, Stack, T, Ts, Tzr);
yeccpars2_28(S, tables, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 30, Ss, Stack, T, Ts, Tzr);
yeccpars2_28(_, _, _, _, T, _, _) ->
 yeccerror(T).

yeccpars2_29(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_|Nss] = Ss,
 NewStack = yeccpars2_29_(Stack),
 yeccgoto_tname(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_30(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_|Nss] = Ss,
 NewStack = yeccpars2_30_(Stack),
 yeccgoto_tname(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccpars2_31/7}).
yeccpars2_31(S, name, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 35, Ss, Stack, T, Ts, Tzr);
yeccpars2_31(_, _, _, _, T, _, _) ->
 yeccerror(T).

-dialyzer({nowarn_function, yeccpars2_32/7}).
yeccpars2_32(S, '=', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 162, Ss, Stack, T, Ts, Tzr);
yeccpars2_32(_, _, _, _, T, _, _) ->
 yeccerror(T).

yeccpars2_33(S, where, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 92, Ss, Stack, T, Ts, Tzr);
yeccpars2_33(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 NewStack = yeccpars2_33_(Stack),
 yeccpars2_91(_S, Cat, [33 | Ss], NewStack, T, Ts, Tzr).

yeccpars2_34(S, ',', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 88, Ss, Stack, T, Ts, Tzr);
yeccpars2_34(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 NewStack = yeccpars2_34_(Stack),
 yeccgoto_assign_commalist(hd(Ss), Cat, Ss, NewStack, T, Ts, Tzr).

yeccpars2_35(S, '.', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 36, Ss, Stack, T, Ts, Tzr);
yeccpars2_35(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 NewStack = yeccpars2_35_(Stack),
 yeccgoto_column(hd(Ss), Cat, Ss, NewStack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccpars2_36/7}).
yeccpars2_36(S, name, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 38, Ss, Stack, T, Ts, Tzr);
yeccpars2_36(_, _, _, _, T, _, _) ->
 yeccerror(T).

-dialyzer({nowarn_function, yeccpars2_37/7}).
yeccpars2_37(S, '=', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 39, Ss, Stack, T, Ts, Tzr);
yeccpars2_37(_, _, _, _, T, _, _) ->
 yeccerror(T).

yeccpars2_38(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 NewStack = yeccpars2_38_(Stack),
 yeccgoto_column(hd(Ss), Cat, Ss, NewStack, T, Ts, Tzr).

yeccpars2_39(S, '(', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 47, Ss, Stack, T, Ts, Tzr);
yeccpars2_39(S, '*', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 48, Ss, Stack, T, Ts, Tzr);
yeccpars2_39(S, cast, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 49, Ss, Stack, T, Ts, Tzr);
yeccpars2_39(S, name, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 51, Ss, Stack, T, Ts, Tzr);
yeccpars2_39(S, null, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 52, Ss, Stack, T, Ts, Tzr);
yeccpars2_39(S, var, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 54, Ss, Stack, T, Ts, Tzr);
yeccpars2_39(S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_122(S, Cat, Ss, Stack, T, Ts, Tzr).

yeccpars2_40(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccgoto_scalar_exp(hd(Ss), Cat, Ss, Stack, T, Ts, Tzr).

yeccpars2_41(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccgoto_column_ref(hd(Ss), Cat, Ss, Stack, T, Ts, Tzr).

yeccpars2_42(S, '*', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 61, Ss, Stack, T, Ts, Tzr);
yeccpars2_42(S, '+', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 62, Ss, Stack, T, Ts, Tzr);
yeccpars2_42(S, '-', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 64, Ss, Stack, T, Ts, Tzr);
yeccpars2_42(S, '/', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 65, Ss, Stack, T, Ts, Tzr);
yeccpars2_42(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_,_,_|Nss] = Ss,
 NewStack = yeccpars2_42_(Stack),
 yeccgoto_assign(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_43(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccgoto_atom(hd(Ss), Cat, Ss, Stack, T, Ts, Tzr).

yeccpars2_44(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccgoto_scalar_exp(hd(Ss), Cat, Ss, Stack, T, Ts, Tzr).

yeccpars2_45(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccgoto_scalar_exp(hd(Ss), Cat, Ss, Stack, T, Ts, Tzr).

yeccpars2_46(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 NewStack = yeccpars2_46_(Stack),
 yeccgoto_scalar_exp(hd(Ss), Cat, Ss, NewStack, T, Ts, Tzr).

%% yeccpars2_47: see yeccpars2_39

yeccpars2_48(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 NewStack = yeccpars2_48_(Stack),
 yeccgoto_scalar_exp(hd(Ss), Cat, Ss, NewStack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccpars2_49/7}).
yeccpars2_49(S, '(', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 72, Ss, Stack, T, Ts, Tzr);
yeccpars2_49(_, _, _, _, T, _, _) ->
 yeccerror(T).

yeccpars2_50(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 NewStack = yeccpars2_50_(Stack),
 yeccgoto_literal(hd(Ss), Cat, Ss, NewStack, T, Ts, Tzr).

yeccpars2_51(S, '(', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 55, Ss, Stack, T, Ts, Tzr);
yeccpars2_51(S, '.', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 56, Ss, Stack, T, Ts, Tzr);
yeccpars2_51(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 NewStack = yeccpars2_51_(Stack),
 yeccgoto_table_column(hd(Ss), Cat, Ss, NewStack, T, Ts, Tzr).

yeccpars2_52(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 NewStack = yeccpars2_52_(Stack),
 yeccgoto_scalar_exp(hd(Ss), Cat, Ss, NewStack, T, Ts, Tzr).

yeccpars2_53(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 NewStack = yeccpars2_53_(Stack),
 yeccgoto_literal(hd(Ss), Cat, Ss, NewStack, T, Ts, Tzr).

yeccpars2_54(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 NewStack = yeccpars2_54_(Stack),
 yeccgoto_variable(hd(Ss), Cat, Ss, NewStack, T, Ts, Tzr).

yeccpars2_55(S, '(', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 47, Ss, Stack, T, Ts, Tzr);
yeccpars2_55(S, '*', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 48, Ss, Stack, T, Ts, Tzr);
yeccpars2_55(S, cast, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 49, Ss, Stack, T, Ts, Tzr);
yeccpars2_55(S, integer, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 50, Ss, Stack, T, Ts, Tzr);
yeccpars2_55(S, name, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 51, Ss, Stack, T, Ts, Tzr);
yeccpars2_55(S, null, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 52, Ss, Stack, T, Ts, Tzr);
yeccpars2_55(S, string, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 53, Ss, Stack, T, Ts, Tzr);
yeccpars2_55(S, var, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 54, Ss, Stack, T, Ts, Tzr);
yeccpars2_55(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 NewStack = yeccpars2_55_(Stack),
 yeccpars2_59(59, Cat, [55 | Ss], NewStack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccpars2_56/7}).
yeccpars2_56(S, '*', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 57, Ss, Stack, T, Ts, Tzr);
yeccpars2_56(S, name, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 58, Ss, Stack, T, Ts, Tzr);
yeccpars2_56(_, _, _, _, T, _, _) ->
 yeccerror(T).

yeccpars2_57(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_|Nss] = Ss,
 NewStack = yeccpars2_57_(Stack),
 yeccgoto_column_ref(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_58(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_|Nss] = Ss,
 NewStack = yeccpars2_58_(Stack),
 yeccgoto_table_column(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccpars2_59/7}).
yeccpars2_59(S, ')', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 71, Ss, Stack, T, Ts, Tzr);
yeccpars2_59(_, _, _, _, T, _, _) ->
 yeccerror(T).

yeccpars2_60(S, '*', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 61, Ss, Stack, T, Ts, Tzr);
yeccpars2_60(S, '+', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 62, Ss, Stack, T, Ts, Tzr);
yeccpars2_60(S, ',', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 63, Ss, Stack, T, Ts, Tzr);
yeccpars2_60(S, '-', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 64, Ss, Stack, T, Ts, Tzr);
yeccpars2_60(S, '/', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 65, Ss, Stack, T, Ts, Tzr);
yeccpars2_60(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 NewStack = yeccpars2_60_(Stack),
 yeccgoto_scalar_exp_commalist(hd(Ss), Cat, Ss, NewStack, T, Ts, Tzr).

%% yeccpars2_61: see yeccpars2_39

%% yeccpars2_62: see yeccpars2_39

yeccpars2_63(S, '(', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 47, Ss, Stack, T, Ts, Tzr);
yeccpars2_63(S, '*', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 48, Ss, Stack, T, Ts, Tzr);
yeccpars2_63(S, cast, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 49, Ss, Stack, T, Ts, Tzr);
yeccpars2_63(S, integer, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 50, Ss, Stack, T, Ts, Tzr);
yeccpars2_63(S, name, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 51, Ss, Stack, T, Ts, Tzr);
yeccpars2_63(S, null, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 52, Ss, Stack, T, Ts, Tzr);
yeccpars2_63(S, string, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 53, Ss, Stack, T, Ts, Tzr);
yeccpars2_63(S, var, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 54, Ss, Stack, T, Ts, Tzr);
yeccpars2_63(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 NewStack = yeccpars2_63_(Stack),
 yeccpars2_68(_S, Cat, [63 | Ss], NewStack, T, Ts, Tzr).

%% yeccpars2_64: see yeccpars2_39

%% yeccpars2_65: see yeccpars2_39

yeccpars2_66(S, '*', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 61, Ss, Stack, T, Ts, Tzr);
yeccpars2_66(S, '+', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 62, Ss, Stack, T, Ts, Tzr);
yeccpars2_66(S, '-', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 64, Ss, Stack, T, Ts, Tzr);
yeccpars2_66(S, '/', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 65, Ss, Stack, T, Ts, Tzr);
yeccpars2_66(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_|Nss] = Ss,
 NewStack = yeccpars2_66_(Stack),
 yeccgoto_scalar_exp(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_67(S, '*', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 61, Ss, Stack, T, Ts, Tzr);
yeccpars2_67(S, '+', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 62, Ss, Stack, T, Ts, Tzr);
yeccpars2_67(S, '-', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 64, Ss, Stack, T, Ts, Tzr);
yeccpars2_67(S, '/', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 65, Ss, Stack, T, Ts, Tzr);
yeccpars2_67(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_|Nss] = Ss,
 NewStack = yeccpars2_67_(Stack),
 yeccgoto_scalar_exp(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_68(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_|Nss] = Ss,
 NewStack = yeccpars2_68_(Stack),
 yeccgoto_scalar_exp_commalist(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_69(S, '*', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 61, Ss, Stack, T, Ts, Tzr);
yeccpars2_69(S, '+', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 62, Ss, Stack, T, Ts, Tzr);
yeccpars2_69(S, '-', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 64, Ss, Stack, T, Ts, Tzr);
yeccpars2_69(S, '/', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 65, Ss, Stack, T, Ts, Tzr);
yeccpars2_69(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_|Nss] = Ss,
 NewStack = yeccpars2_69_(Stack),
 yeccgoto_scalar_exp(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_70(S, '*', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 61, Ss, Stack, T, Ts, Tzr);
yeccpars2_70(S, '+', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 62, Ss, Stack, T, Ts, Tzr);
yeccpars2_70(S, '-', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 64, Ss, Stack, T, Ts, Tzr);
yeccpars2_70(S, '/', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 65, Ss, Stack, T, Ts, Tzr);
yeccpars2_70(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_|Nss] = Ss,
 NewStack = yeccpars2_70_(Stack),
 yeccgoto_scalar_exp(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_71(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_,_|Nss] = Ss,
 NewStack = yeccpars2_71_(Stack),
 yeccgoto_scalar_exp(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

%% yeccpars2_72: see yeccpars2_39

yeccpars2_73(S, as, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 74, Ss, Stack, T, Ts, Tzr);
yeccpars2_73(S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_cont_73(S, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccpars2_73/7}).
yeccpars2_cont_73(S, '*', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 61, Ss, Stack, T, Ts, Tzr);
yeccpars2_cont_73(S, '+', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 62, Ss, Stack, T, Ts, Tzr);
yeccpars2_cont_73(S, '-', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 64, Ss, Stack, T, Ts, Tzr);
yeccpars2_cont_73(S, '/', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 65, Ss, Stack, T, Ts, Tzr);
yeccpars2_cont_73(_, _, _, _, T, _, _) ->
 yeccerror(T).

-dialyzer({nowarn_function, yeccpars2_74/7}).
yeccpars2_74(S, name, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 75, Ss, Stack, T, Ts, Tzr);
yeccpars2_74(_, _, _, _, T, _, _) ->
 yeccerror(T).

yeccpars2_75(S, '(', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 77, Ss, Stack, T, Ts, Tzr);
yeccpars2_75(S, character, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 78, Ss, Stack, T, Ts, Tzr);
yeccpars2_75(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 NewStack = yeccpars2_75_(Stack),
 yeccpars2_76(76, Cat, [75 | Ss], NewStack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccpars2_76/7}).
yeccpars2_76(S, ')', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 85, Ss, Stack, T, Ts, Tzr);
yeccpars2_76(_, _, _, _, T, _, _) ->
 yeccerror(T).

-dialyzer({nowarn_function, yeccpars2_77/7}).
yeccpars2_77(S, integer, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 81, Ss, Stack, T, Ts, Tzr);
yeccpars2_77(_, _, _, _, T, _, _) ->
 yeccerror(T).

-dialyzer({nowarn_function, yeccpars2_78/7}).
yeccpars2_78(S, set, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 79, Ss, Stack, T, Ts, Tzr);
yeccpars2_78(_, _, _, _, T, _, _) ->
 yeccerror(T).

-dialyzer({nowarn_function, yeccpars2_79/7}).
yeccpars2_79(S, name, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 80, Ss, Stack, T, Ts, Tzr);
yeccpars2_79(_, _, _, _, T, _, _) ->
 yeccerror(T).

yeccpars2_80(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_|Nss] = Ss,
 NewStack = yeccpars2_80_(Stack),
 yeccgoto_opt_charset(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccpars2_81/7}).
yeccpars2_81(S, ')', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 82, Ss, Stack, T, Ts, Tzr);
yeccpars2_81(_, _, _, _, T, _, _) ->
 yeccerror(T).

yeccpars2_82(S, character, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 78, Ss, Stack, T, Ts, Tzr);
yeccpars2_82(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 NewStack = yeccpars2_82_(Stack),
 yeccpars2_83(83, Cat, [82 | Ss], NewStack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccpars2_83/7}).
yeccpars2_83(S, ')', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 84, Ss, Stack, T, Ts, Tzr);
yeccpars2_83(_, _, _, _, T, _, _) ->
 yeccerror(T).

yeccpars2_84(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_,_,_,_,_,_,_,_|Nss] = Ss,
 NewStack = yeccpars2_84_(Stack),
 yeccgoto_cast_expr(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_85(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_,_,_,_,_|Nss] = Ss,
 NewStack = yeccpars2_85_(Stack),
 yeccgoto_cast_expr(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_86(S, ')', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 87, Ss, Stack, T, Ts, Tzr);
yeccpars2_86(S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_cont_73(S, Cat, Ss, Stack, T, Ts, Tzr).

yeccpars2_87(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_|Nss] = Ss,
 NewStack = yeccpars2_87_(Stack),
 yeccgoto_scalar_exp(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

%% yeccpars2_88: see yeccpars2_31

yeccpars2_89(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_|Nss] = Ss,
 NewStack = yeccpars2_89_(Stack),
 yeccgoto_assign_commalist(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_90(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccgoto_opt_where_clause(hd(Ss), Cat, Ss, Stack, T, Ts, Tzr).

yeccpars2_91(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_,_,_|Nss] = Ss,
 NewStack = yeccpars2_91_(Stack),
 yeccgoto_update_stmt(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_92(S, '(', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 102, Ss, Stack, T, Ts, Tzr);
yeccpars2_92(S, '*', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 48, Ss, Stack, T, Ts, Tzr);
yeccpars2_92(S, cast, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 49, Ss, Stack, T, Ts, Tzr);
yeccpars2_92(S, name, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 51, Ss, Stack, T, Ts, Tzr);
yeccpars2_92(S, 'not', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 103, Ss, Stack, T, Ts, Tzr);
yeccpars2_92(S, null, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 52, Ss, Stack, T, Ts, Tzr);
yeccpars2_92(S, var, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 54, Ss, Stack, T, Ts, Tzr);
yeccpars2_92(S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_122(S, Cat, Ss, Stack, T, Ts, Tzr).

yeccpars2_93(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccgoto_predicate(hd(Ss), Cat, Ss, Stack, T, Ts, Tzr).

yeccpars2_94(S, 'and', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 105, Ss, Stack, T, Ts, Tzr);
yeccpars2_94(S, 'or', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 106, Ss, Stack, T, Ts, Tzr);
yeccpars2_94(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_|Nss] = Ss,
 NewStack = yeccpars2_94_(Stack),
 yeccgoto_where_clause(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_95(S, '!=', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 111, Ss, Stack, T, Ts, Tzr);
yeccpars2_95(S, '<', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 112, Ss, Stack, T, Ts, Tzr);
yeccpars2_95(S, '<=', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 113, Ss, Stack, T, Ts, Tzr);
yeccpars2_95(S, '<>', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 114, Ss, Stack, T, Ts, Tzr);
yeccpars2_95(S, '=', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 115, Ss, Stack, T, Ts, Tzr);
yeccpars2_95(S, '=<', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 116, Ss, Stack, T, Ts, Tzr);
yeccpars2_95(S, '=>', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 117, Ss, Stack, T, Ts, Tzr);
yeccpars2_95(S, '>', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 118, Ss, Stack, T, Ts, Tzr);
yeccpars2_95(S, '>=', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 119, Ss, Stack, T, Ts, Tzr);
yeccpars2_95(S, between, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 120, Ss, Stack, T, Ts, Tzr);
yeccpars2_95(S, in, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 121, Ss, Stack, T, Ts, Tzr);
yeccpars2_95(S, like, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 122, Ss, Stack, T, Ts, Tzr);
yeccpars2_95(S, 'not', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 123, Ss, Stack, T, Ts, Tzr);
yeccpars2_95(S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_cont_73(S, Cat, Ss, Stack, T, Ts, Tzr).

yeccpars2_96(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccgoto_search_cond(hd(Ss), Cat, Ss, Stack, T, Ts, Tzr).

yeccpars2_97(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccgoto_predicate(hd(Ss), Cat, Ss, Stack, T, Ts, Tzr).

yeccpars2_98(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccgoto_predicate(hd(Ss), Cat, Ss, Stack, T, Ts, Tzr).

yeccpars2_99(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccgoto_predicate(hd(Ss), Cat, Ss, Stack, T, Ts, Tzr).

yeccpars2_100(S, is, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 158, Ss, Stack, T, Ts, Tzr);
yeccpars2_100(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccgoto_scalar_exp(hd(Ss), Cat, Ss, Stack, T, Ts, Tzr).

yeccpars2_101(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccgoto_predicate(hd(Ss), Cat, Ss, Stack, T, Ts, Tzr).

%% yeccpars2_102: see yeccpars2_92

%% yeccpars2_103: see yeccpars2_92

yeccpars2_104(S, 'and', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 105, Ss, Stack, T, Ts, Tzr);
yeccpars2_104(S, 'or', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 106, Ss, Stack, T, Ts, Tzr);
yeccpars2_104(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_|Nss] = Ss,
 NewStack = yeccpars2_104_(Stack),
 yeccgoto_search_cond(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

%% yeccpars2_105: see yeccpars2_92

%% yeccpars2_106: see yeccpars2_92

yeccpars2_107(S, 'and', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 105, Ss, Stack, T, Ts, Tzr);
yeccpars2_107(S, 'or', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 106, Ss, Stack, T, Ts, Tzr);
yeccpars2_107(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_|Nss] = Ss,
 NewStack = yeccpars2_107_(Stack),
 yeccgoto_search_cond(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_108(S, 'and', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 105, Ss, Stack, T, Ts, Tzr);
yeccpars2_108(S, 'or', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 106, Ss, Stack, T, Ts, Tzr);
yeccpars2_108(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_|Nss] = Ss,
 NewStack = yeccpars2_108_(Stack),
 yeccgoto_search_cond(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccpars2_109/7}).
yeccpars2_109(S, ')', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 157, Ss, Stack, T, Ts, Tzr);
yeccpars2_109(S, 'and', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 105, Ss, Stack, T, Ts, Tzr);
yeccpars2_109(S, 'or', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 106, Ss, Stack, T, Ts, Tzr);
yeccpars2_109(_, _, _, _, T, _, _) ->
 yeccerror(T).

yeccpars2_110(S, '!=', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 111, Ss, Stack, T, Ts, Tzr);
yeccpars2_110(S, ')', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 87, Ss, Stack, T, Ts, Tzr);
yeccpars2_110(S, '<', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 112, Ss, Stack, T, Ts, Tzr);
yeccpars2_110(S, '<=', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 113, Ss, Stack, T, Ts, Tzr);
yeccpars2_110(S, '<>', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 114, Ss, Stack, T, Ts, Tzr);
yeccpars2_110(S, '=', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 115, Ss, Stack, T, Ts, Tzr);
yeccpars2_110(S, '=<', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 116, Ss, Stack, T, Ts, Tzr);
yeccpars2_110(S, '=>', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 117, Ss, Stack, T, Ts, Tzr);
yeccpars2_110(S, '>', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 118, Ss, Stack, T, Ts, Tzr);
yeccpars2_110(S, '>=', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 119, Ss, Stack, T, Ts, Tzr);
yeccpars2_110(S, between, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 120, Ss, Stack, T, Ts, Tzr);
yeccpars2_110(S, in, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 121, Ss, Stack, T, Ts, Tzr);
yeccpars2_110(S, like, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 122, Ss, Stack, T, Ts, Tzr);
yeccpars2_110(S, 'not', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 123, Ss, Stack, T, Ts, Tzr);
yeccpars2_110(S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_cont_73(S, Cat, Ss, Stack, T, Ts, Tzr).

%% yeccpars2_111: see yeccpars2_39

%% yeccpars2_112: see yeccpars2_39

%% yeccpars2_113: see yeccpars2_39

%% yeccpars2_114: see yeccpars2_39

%% yeccpars2_115: see yeccpars2_39

%% yeccpars2_116: see yeccpars2_39

%% yeccpars2_117: see yeccpars2_39

%% yeccpars2_118: see yeccpars2_39

%% yeccpars2_119: see yeccpars2_39

%% yeccpars2_120: see yeccpars2_39

-dialyzer({nowarn_function, yeccpars2_121/7}).
yeccpars2_121(S, '(', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 140, Ss, Stack, T, Ts, Tzr);
yeccpars2_121(_, _, _, _, T, _, _) ->
 yeccerror(T).

-dialyzer({nowarn_function, yeccpars2_122/7}).
yeccpars2_122(S, integer, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 50, Ss, Stack, T, Ts, Tzr);
yeccpars2_122(S, string, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 53, Ss, Stack, T, Ts, Tzr);
yeccpars2_122(_, _, _, _, T, _, _) ->
 yeccerror(T).

-dialyzer({nowarn_function, yeccpars2_123/7}).
yeccpars2_123(S, between, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 124, Ss, Stack, T, Ts, Tzr);
yeccpars2_123(S, in, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 125, Ss, Stack, T, Ts, Tzr);
yeccpars2_123(S, like, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 126, Ss, Stack, T, Ts, Tzr);
yeccpars2_123(_, _, _, _, T, _, _) ->
 yeccerror(T).

%% yeccpars2_124: see yeccpars2_39

-dialyzer({nowarn_function, yeccpars2_125/7}).
yeccpars2_125(S, '(', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 128, Ss, Stack, T, Ts, Tzr);
yeccpars2_125(_, _, _, _, T, _, _) ->
 yeccerror(T).

%% yeccpars2_126: see yeccpars2_122

yeccpars2_127(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_,_|Nss] = Ss,
 NewStack = yeccpars2_127_(Stack),
 yeccgoto_like_pred(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_128(S, select, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 19, Ss, Stack, T, Ts, Tzr);
yeccpars2_128(S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_122(S, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccpars2_129/7}).
yeccpars2_129(S, ')', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 135, Ss, Stack, T, Ts, Tzr);
yeccpars2_129(_, _, _, _, T, _, _) ->
 yeccerror(T).

-dialyzer({nowarn_function, yeccpars2_130/7}).
yeccpars2_130(S, ')', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 134, Ss, Stack, T, Ts, Tzr);
yeccpars2_130(_, _, _, _, T, _, _) ->
 yeccerror(T).

yeccpars2_131(S, ',', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 132, Ss, Stack, T, Ts, Tzr);
yeccpars2_131(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 NewStack = yeccpars2_131_(Stack),
 yeccgoto_atom_commalist(hd(Ss), Cat, Ss, NewStack, T, Ts, Tzr).

%% yeccpars2_132: see yeccpars2_122

yeccpars2_133(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_|Nss] = Ss,
 NewStack = yeccpars2_133_(Stack),
 yeccgoto_atom_commalist(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_134(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_,_,_,_|Nss] = Ss,
 NewStack = yeccpars2_134_(Stack),
 yeccgoto_in_pred(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_135(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_,_,_,_|Nss] = Ss,
 NewStack = yeccpars2_135_(Stack),
 yeccgoto_in_pred(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_136(S, 'and', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 137, Ss, Stack, T, Ts, Tzr);
yeccpars2_136(S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_cont_73(S, Cat, Ss, Stack, T, Ts, Tzr).

%% yeccpars2_137: see yeccpars2_39

yeccpars2_138(S, '*', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 61, Ss, Stack, T, Ts, Tzr);
yeccpars2_138(S, '+', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 62, Ss, Stack, T, Ts, Tzr);
yeccpars2_138(S, '-', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 64, Ss, Stack, T, Ts, Tzr);
yeccpars2_138(S, '/', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 65, Ss, Stack, T, Ts, Tzr);
yeccpars2_138(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_,_,_,_|Nss] = Ss,
 NewStack = yeccpars2_138_(Stack),
 yeccgoto_between_pred(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_139(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_|Nss] = Ss,
 NewStack = yeccpars2_139_(Stack),
 yeccgoto_like_pred(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

%% yeccpars2_140: see yeccpars2_128

-dialyzer({nowarn_function, yeccpars2_141/7}).
yeccpars2_141(S, ')', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 144, Ss, Stack, T, Ts, Tzr);
yeccpars2_141(_, _, _, _, T, _, _) ->
 yeccerror(T).

-dialyzer({nowarn_function, yeccpars2_142/7}).
yeccpars2_142(S, ')', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 143, Ss, Stack, T, Ts, Tzr);
yeccpars2_142(_, _, _, _, T, _, _) ->
 yeccerror(T).

yeccpars2_143(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_,_,_|Nss] = Ss,
 NewStack = yeccpars2_143_(Stack),
 yeccgoto_in_pred(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_144(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_,_,_|Nss] = Ss,
 NewStack = yeccpars2_144_(Stack),
 yeccgoto_in_pred(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_145(S, 'and', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 146, Ss, Stack, T, Ts, Tzr);
yeccpars2_145(S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_cont_73(S, Cat, Ss, Stack, T, Ts, Tzr).

%% yeccpars2_146: see yeccpars2_39

yeccpars2_147(S, '*', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 61, Ss, Stack, T, Ts, Tzr);
yeccpars2_147(S, '+', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 62, Ss, Stack, T, Ts, Tzr);
yeccpars2_147(S, '-', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 64, Ss, Stack, T, Ts, Tzr);
yeccpars2_147(S, '/', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 65, Ss, Stack, T, Ts, Tzr);
yeccpars2_147(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_,_,_|Nss] = Ss,
 NewStack = yeccpars2_147_(Stack),
 yeccgoto_between_pred(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_148(S, '*', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 61, Ss, Stack, T, Ts, Tzr);
yeccpars2_148(S, '+', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 62, Ss, Stack, T, Ts, Tzr);
yeccpars2_148(S, '-', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 64, Ss, Stack, T, Ts, Tzr);
yeccpars2_148(S, '/', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 65, Ss, Stack, T, Ts, Tzr);
yeccpars2_148(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_|Nss] = Ss,
 NewStack = yeccpars2_148_(Stack),
 yeccgoto_comparsion_pred(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_149(S, '*', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 61, Ss, Stack, T, Ts, Tzr);
yeccpars2_149(S, '+', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 62, Ss, Stack, T, Ts, Tzr);
yeccpars2_149(S, '-', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 64, Ss, Stack, T, Ts, Tzr);
yeccpars2_149(S, '/', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 65, Ss, Stack, T, Ts, Tzr);
yeccpars2_149(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_|Nss] = Ss,
 NewStack = yeccpars2_149_(Stack),
 yeccgoto_comparsion_pred(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_150(S, '*', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 61, Ss, Stack, T, Ts, Tzr);
yeccpars2_150(S, '+', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 62, Ss, Stack, T, Ts, Tzr);
yeccpars2_150(S, '-', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 64, Ss, Stack, T, Ts, Tzr);
yeccpars2_150(S, '/', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 65, Ss, Stack, T, Ts, Tzr);
yeccpars2_150(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_|Nss] = Ss,
 NewStack = yeccpars2_150_(Stack),
 yeccgoto_comparsion_pred(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_151(S, '*', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 61, Ss, Stack, T, Ts, Tzr);
yeccpars2_151(S, '+', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 62, Ss, Stack, T, Ts, Tzr);
yeccpars2_151(S, '-', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 64, Ss, Stack, T, Ts, Tzr);
yeccpars2_151(S, '/', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 65, Ss, Stack, T, Ts, Tzr);
yeccpars2_151(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_|Nss] = Ss,
 NewStack = yeccpars2_151_(Stack),
 yeccgoto_comparsion_pred(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_152(S, '*', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 61, Ss, Stack, T, Ts, Tzr);
yeccpars2_152(S, '+', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 62, Ss, Stack, T, Ts, Tzr);
yeccpars2_152(S, '-', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 64, Ss, Stack, T, Ts, Tzr);
yeccpars2_152(S, '/', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 65, Ss, Stack, T, Ts, Tzr);
yeccpars2_152(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_|Nss] = Ss,
 NewStack = yeccpars2_152_(Stack),
 yeccgoto_comparsion_pred(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_153(S, '*', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 61, Ss, Stack, T, Ts, Tzr);
yeccpars2_153(S, '+', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 62, Ss, Stack, T, Ts, Tzr);
yeccpars2_153(S, '-', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 64, Ss, Stack, T, Ts, Tzr);
yeccpars2_153(S, '/', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 65, Ss, Stack, T, Ts, Tzr);
yeccpars2_153(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_|Nss] = Ss,
 NewStack = yeccpars2_153_(Stack),
 yeccgoto_comparsion_pred(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_154(S, '*', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 61, Ss, Stack, T, Ts, Tzr);
yeccpars2_154(S, '+', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 62, Ss, Stack, T, Ts, Tzr);
yeccpars2_154(S, '-', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 64, Ss, Stack, T, Ts, Tzr);
yeccpars2_154(S, '/', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 65, Ss, Stack, T, Ts, Tzr);
yeccpars2_154(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_|Nss] = Ss,
 NewStack = yeccpars2_154_(Stack),
 yeccgoto_comparsion_pred(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_155(S, '*', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 61, Ss, Stack, T, Ts, Tzr);
yeccpars2_155(S, '+', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 62, Ss, Stack, T, Ts, Tzr);
yeccpars2_155(S, '-', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 64, Ss, Stack, T, Ts, Tzr);
yeccpars2_155(S, '/', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 65, Ss, Stack, T, Ts, Tzr);
yeccpars2_155(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_|Nss] = Ss,
 NewStack = yeccpars2_155_(Stack),
 yeccgoto_comparsion_pred(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_156(S, '*', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 61, Ss, Stack, T, Ts, Tzr);
yeccpars2_156(S, '+', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 62, Ss, Stack, T, Ts, Tzr);
yeccpars2_156(S, '-', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 64, Ss, Stack, T, Ts, Tzr);
yeccpars2_156(S, '/', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 65, Ss, Stack, T, Ts, Tzr);
yeccpars2_156(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_|Nss] = Ss,
 NewStack = yeccpars2_156_(Stack),
 yeccgoto_comparsion_pred(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_157(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_|Nss] = Ss,
 NewStack = yeccpars2_157_(Stack),
 yeccgoto_search_cond(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_158(S, '(', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 47, Ss, Stack, T, Ts, Tzr);
yeccpars2_158(S, '*', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 48, Ss, Stack, T, Ts, Tzr);
yeccpars2_158(S, cast, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 49, Ss, Stack, T, Ts, Tzr);
yeccpars2_158(S, name, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 51, Ss, Stack, T, Ts, Tzr);
yeccpars2_158(S, 'not', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 160, Ss, Stack, T, Ts, Tzr);
yeccpars2_158(S, null, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 52, Ss, Stack, T, Ts, Tzr);
yeccpars2_158(S, var, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 54, Ss, Stack, T, Ts, Tzr);
yeccpars2_158(S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_122(S, Cat, Ss, Stack, T, Ts, Tzr).

yeccpars2_159(S, '*', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 61, Ss, Stack, T, Ts, Tzr);
yeccpars2_159(S, '+', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 62, Ss, Stack, T, Ts, Tzr);
yeccpars2_159(S, '-', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 64, Ss, Stack, T, Ts, Tzr);
yeccpars2_159(S, '/', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 65, Ss, Stack, T, Ts, Tzr);
yeccpars2_159(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_|Nss] = Ss,
 NewStack = yeccpars2_159_(Stack),
 yeccgoto_test_for_null_pred(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

%% yeccpars2_160: see yeccpars2_39

yeccpars2_161(S, '*', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 61, Ss, Stack, T, Ts, Tzr);
yeccpars2_161(S, '+', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 62, Ss, Stack, T, Ts, Tzr);
yeccpars2_161(S, '-', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 64, Ss, Stack, T, Ts, Tzr);
yeccpars2_161(S, '/', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 65, Ss, Stack, T, Ts, Tzr);
yeccpars2_161(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_,_|Nss] = Ss,
 NewStack = yeccpars2_161_(Stack),
 yeccgoto_test_for_null_pred(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

%% yeccpars2_162: see yeccpars2_39

yeccpars2_163(S, '*', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 61, Ss, Stack, T, Ts, Tzr);
yeccpars2_163(S, '+', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 62, Ss, Stack, T, Ts, Tzr);
yeccpars2_163(S, '-', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 64, Ss, Stack, T, Ts, Tzr);
yeccpars2_163(S, '/', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 65, Ss, Stack, T, Ts, Tzr);
yeccpars2_163(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_|Nss] = Ss,
 NewStack = yeccpars2_163_(Stack),
 yeccgoto_assign(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccpars2_164/7}).
yeccpars2_164(S, name, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 166, Ss, Stack, T, Ts, Tzr);
yeccpars2_164(_, _, _, _, T, _, _) ->
 yeccerror(T).

yeccpars2_165(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_|Nss] = Ss,
 NewStack = yeccpars2_165_(Stack),
 yeccgoto_table_name(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_166(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_|Nss] = Ss,
 NewStack = yeccpars2_166_(Stack),
 yeccgoto_table_name(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccpars2_167/7}).
yeccpars2_167(S, collation, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 170, Ss, Stack, T, Ts, Tzr);
yeccpars2_167(S, create, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 171, Ss, Stack, T, Ts, Tzr);
yeccpars2_167(S, databases, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 172, Ss, Stack, T, Ts, Tzr);
yeccpars2_167(S, fields, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 173, Ss, Stack, T, Ts, Tzr);
yeccpars2_167(S, index, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 174, Ss, Stack, T, Ts, Tzr);
yeccpars2_167(S, tables, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 175, Ss, Stack, T, Ts, Tzr);
yeccpars2_167(S, variables, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 176, Ss, Stack, T, Ts, Tzr);
yeccpars2_167(_, _, _, _, T, _, _) ->
 yeccerror(T).

yeccpars2_168(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 NewStack = yeccpars2_168_(Stack),
 yeccgoto_opt_full(hd(Ss), Cat, Ss, NewStack, T, Ts, Tzr).

yeccpars2_169(S, from, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 180, Ss, Stack, T, Ts, Tzr);
yeccpars2_169(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 NewStack = yeccpars2_169_(Stack),
 yeccpars2_179(179, Cat, [169 | Ss], NewStack, T, Ts, Tzr).

yeccpars2_170(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 NewStack = yeccpars2_170_(Stack),
 yeccgoto_show_type(hd(Ss), Cat, Ss, NewStack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccpars2_171/7}).
yeccpars2_171(S, table, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 177, Ss, Stack, T, Ts, Tzr);
yeccpars2_171(_, _, _, _, T, _, _) ->
 yeccerror(T).

yeccpars2_172(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 NewStack = yeccpars2_172_(Stack),
 yeccgoto_show_type(hd(Ss), Cat, Ss, NewStack, T, Ts, Tzr).

yeccpars2_173(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 NewStack = yeccpars2_173_(Stack),
 yeccgoto_show_type(hd(Ss), Cat, Ss, NewStack, T, Ts, Tzr).

yeccpars2_174(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 NewStack = yeccpars2_174_(Stack),
 yeccgoto_show_type(hd(Ss), Cat, Ss, NewStack, T, Ts, Tzr).

yeccpars2_175(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 NewStack = yeccpars2_175_(Stack),
 yeccgoto_show_type(hd(Ss), Cat, Ss, NewStack, T, Ts, Tzr).

yeccpars2_176(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 NewStack = yeccpars2_176_(Stack),
 yeccgoto_show_type(hd(Ss), Cat, Ss, NewStack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccpars2_177/7}).
yeccpars2_177(S, name, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 178, Ss, Stack, T, Ts, Tzr);
yeccpars2_177(_, _, _, _, T, _, _) ->
 yeccerror(T).

yeccpars2_178(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_,_,_|Nss] = Ss,
 NewStack = yeccpars2_178_(Stack),
 yeccgoto_show_stmt(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_179(S, like, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 184, Ss, Stack, T, Ts, Tzr);
yeccpars2_179(S, where, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 92, Ss, Stack, T, Ts, Tzr);
yeccpars2_179(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 NewStack = yeccpars2_179_(Stack),
 yeccpars2_183(_S, Cat, [179 | Ss], NewStack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccpars2_180/7}).
yeccpars2_180(S, name, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 181, Ss, Stack, T, Ts, Tzr);
yeccpars2_180(_, _, _, _, T, _, _) ->
 yeccerror(T).

yeccpars2_181(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_|Nss] = Ss,
 NewStack = yeccpars2_181_(Stack),
 yeccgoto_opt_from(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_182(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccgoto_opt_condition(hd(Ss), Cat, Ss, Stack, T, Ts, Tzr).

yeccpars2_183(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_,_,_|Nss] = Ss,
 NewStack = yeccpars2_183_(Stack),
 yeccgoto_show_stmt(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

%% yeccpars2_184: see yeccpars2_122

yeccpars2_185(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_|Nss] = Ss,
 NewStack = yeccpars2_185_(Stack),
 yeccgoto_opt_condition(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_186(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccgoto_set_target(hd(Ss), Cat, Ss, Stack, T, Ts, Tzr).

yeccpars2_187(S, '=', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 195, Ss, Stack, T, Ts, Tzr);
yeccpars2_187(S, name, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 196, Ss, Stack, T, Ts, Tzr);
yeccpars2_187(S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_122(S, Cat, Ss, Stack, T, Ts, Tzr).

yeccpars2_188(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_|Nss] = Ss,
 NewStack = yeccpars2_188_(Stack),
 yeccgoto_set_stmt(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_189(S, ',', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 191, Ss, Stack, T, Ts, Tzr);
yeccpars2_189(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 NewStack = yeccpars2_189_(Stack),
 yeccgoto_set_assign_list(hd(Ss), Cat, Ss, NewStack, T, Ts, Tzr).

yeccpars2_190(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 NewStack = yeccpars2_190_(Stack),
 yeccgoto_set_target(hd(Ss), Cat, Ss, NewStack, T, Ts, Tzr).

%% yeccpars2_191: see yeccpars2_20

yeccpars2_192(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_|Nss] = Ss,
 NewStack = yeccpars2_192_(Stack),
 yeccgoto_set_assign_list(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_193(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_|Nss] = Ss,
 NewStack = yeccpars2_193_(Stack),
 yeccgoto_set_assign(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_194(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 NewStack = yeccpars2_194_(Stack),
 yeccgoto_set_value(hd(Ss), Cat, Ss, NewStack, T, Ts, Tzr).

%% yeccpars2_195: see yeccpars2_39

yeccpars2_196(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 NewStack = yeccpars2_196_(Stack),
 yeccgoto_set_value(hd(Ss), Cat, Ss, NewStack, T, Ts, Tzr).

yeccpars2_197(S, '*', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 61, Ss, Stack, T, Ts, Tzr);
yeccpars2_197(S, '+', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 62, Ss, Stack, T, Ts, Tzr);
yeccpars2_197(S, '-', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 64, Ss, Stack, T, Ts, Tzr);
yeccpars2_197(S, '/', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 65, Ss, Stack, T, Ts, Tzr);
yeccpars2_197(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_|Nss] = Ss,
 NewStack = yeccpars2_197_(Stack),
 yeccgoto_set_assign(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_198(S, from, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 214, Ss, Stack, T, Ts, Tzr);
yeccpars2_198(S, limit, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 215, Ss, Stack, T, Ts, Tzr);
yeccpars2_198(S, offset, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 216, Ss, Stack, T, Ts, Tzr);
yeccpars2_198(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 NewStack = yeccpars2_198_(Stack),
 yeccpars2_212(_S, Cat, [198 | Ss], NewStack, T, Ts, Tzr).

yeccpars2_199(S, ',', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 210, Ss, Stack, T, Ts, Tzr);
yeccpars2_199(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 NewStack = yeccpars2_199_(Stack),
 yeccgoto_selection_list(hd(Ss), Cat, Ss, NewStack, T, Ts, Tzr).

yeccpars2_200(S, as, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 208, Ss, Stack, T, Ts, Tzr);
yeccpars2_200(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccgoto_selection_item_alias(hd(Ss), Cat, Ss, Stack, T, Ts, Tzr).

yeccpars2_201(S, '*', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 61, Ss, Stack, T, Ts, Tzr);
yeccpars2_201(S, '+', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 62, Ss, Stack, T, Ts, Tzr);
yeccpars2_201(S, '-', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 64, Ss, Stack, T, Ts, Tzr);
yeccpars2_201(S, '/', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 65, Ss, Stack, T, Ts, Tzr);
yeccpars2_201(S, collate, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 206, Ss, Stack, T, Ts, Tzr);
yeccpars2_201(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 NewStack = yeccpars2_201_(Stack),
 yeccpars2_205(_S, Cat, [201 | Ss], NewStack, T, Ts, Tzr).

yeccpars2_202(S, '(', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 47, Ss, Stack, T, Ts, Tzr);
yeccpars2_202(S, '*', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 48, Ss, Stack, T, Ts, Tzr);
yeccpars2_202(S, cast, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 49, Ss, Stack, T, Ts, Tzr);
yeccpars2_202(S, name, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 51, Ss, Stack, T, Ts, Tzr);
yeccpars2_202(S, null, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 52, Ss, Stack, T, Ts, Tzr);
yeccpars2_202(S, select, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 19, Ss, Stack, T, Ts, Tzr);
yeccpars2_202(S, var, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 54, Ss, Stack, T, Ts, Tzr);
yeccpars2_202(S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_122(S, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccpars2_203/7}).
yeccpars2_203(S, ')', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 204, Ss, Stack, T, Ts, Tzr);
yeccpars2_203(_, _, _, _, T, _, _) ->
 yeccerror(T).

yeccpars2_204(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_|Nss] = Ss,
 NewStack = yeccpars2_204_(Stack),
 yeccgoto_selection_item(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_205(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_|Nss] = Ss,
 NewStack = yeccpars2_205_(Stack),
 yeccgoto_selection_item(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccpars2_206/7}).
yeccpars2_206(S, name, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 207, Ss, Stack, T, Ts, Tzr);
yeccpars2_206(_, _, _, _, T, _, _) ->
 yeccerror(T).

yeccpars2_207(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_|Nss] = Ss,
 NewStack = yeccpars2_207_(Stack),
 yeccgoto_opt_collate(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccpars2_208/7}).
yeccpars2_208(S, name, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 209, Ss, Stack, T, Ts, Tzr);
yeccpars2_208(_, _, _, _, T, _, _) ->
 yeccerror(T).

yeccpars2_209(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_|Nss] = Ss,
 NewStack = yeccpars2_209_(Stack),
 yeccgoto_selection_item_alias(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

%% yeccpars2_210: see yeccpars2_19

yeccpars2_211(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_|Nss] = Ss,
 NewStack = yeccpars2_211_(Stack),
 yeccgoto_selection_list(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_212(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_|Nss] = Ss,
 NewStack = yeccpars2_212_(Stack),
 yeccgoto_select_stmt(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_213(S, where, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 92, Ss, Stack, T, Ts, Tzr);
yeccpars2_213(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 NewStack = yeccpars2_213_(Stack),
 yeccpars2_235(235, Cat, [213 | Ss], NewStack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccpars2_214/7}).
yeccpars2_214(S, '(', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 227, Ss, Stack, T, Ts, Tzr);
yeccpars2_214(S, name, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 27, Ss, Stack, T, Ts, Tzr);
yeccpars2_214(_, _, _, _, T, _, _) ->
 yeccerror(T).

%% yeccpars2_215: see yeccpars2_122

%% yeccpars2_216: see yeccpars2_122

yeccpars2_217(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_|Nss] = Ss,
 NewStack = yeccpars2_217_(Stack),
 yeccgoto_opt_limit_offset_clause(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_218(S, ',', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 219, Ss, Stack, T, Ts, Tzr);
yeccpars2_218(S, offset, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 220, Ss, Stack, T, Ts, Tzr);
yeccpars2_218(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_|Nss] = Ss,
 NewStack = yeccpars2_218_(Stack),
 yeccgoto_opt_limit_offset_clause(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

%% yeccpars2_219: see yeccpars2_122

%% yeccpars2_220: see yeccpars2_122

yeccpars2_221(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_,_|Nss] = Ss,
 NewStack = yeccpars2_221_(Stack),
 yeccgoto_opt_limit_offset_clause(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_222(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_,_|Nss] = Ss,
 NewStack = yeccpars2_222_(Stack),
 yeccgoto_opt_limit_offset_clause(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_223(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_|Nss] = Ss,
 NewStack = yeccpars2_223_(Stack),
 yeccgoto_from_clause(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_224(S, ',', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 232, Ss, Stack, T, Ts, Tzr);
yeccpars2_224(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 NewStack = yeccpars2_224_(Stack),
 yeccgoto_table_ref_commalist(hd(Ss), Cat, Ss, NewStack, T, Ts, Tzr).

yeccpars2_225(S, as, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 230, Ss, Stack, T, Ts, Tzr);
yeccpars2_225(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccgoto_table_ref_alias(hd(Ss), Cat, Ss, Stack, T, Ts, Tzr).

yeccpars2_226(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccgoto_table_ref(hd(Ss), Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccpars2_227/7}).
yeccpars2_227(S, select, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 19, Ss, Stack, T, Ts, Tzr);
yeccpars2_227(_, _, _, _, T, _, _) ->
 yeccerror(T).

-dialyzer({nowarn_function, yeccpars2_228/7}).
yeccpars2_228(S, ')', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 229, Ss, Stack, T, Ts, Tzr);
yeccpars2_228(_, _, _, _, T, _, _) ->
 yeccerror(T).

yeccpars2_229(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_|Nss] = Ss,
 NewStack = yeccpars2_229_(Stack),
 yeccgoto_table_ref(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccpars2_230/7}).
yeccpars2_230(S, name, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 231, Ss, Stack, T, Ts, Tzr);
yeccpars2_230(_, _, _, _, T, _, _) ->
 yeccerror(T).

yeccpars2_231(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_|Nss] = Ss,
 NewStack = yeccpars2_231_(Stack),
 yeccgoto_table_ref_alias(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

%% yeccpars2_232: see yeccpars2_214

yeccpars2_233(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_|Nss] = Ss,
 NewStack = yeccpars2_233_(Stack),
 yeccgoto_table_ref_commalist(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_234(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_,_|Nss] = Ss,
 NewStack = yeccpars2_234_(Stack),
 yeccgoto_select_stmt(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_235(S, order, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 237, Ss, Stack, T, Ts, Tzr);
yeccpars2_235(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 NewStack = yeccpars2_235_(Stack),
 yeccpars2_236(236, Cat, [235 | Ss], NewStack, T, Ts, Tzr).

yeccpars2_236(S, group, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 252, Ss, Stack, T, Ts, Tzr);
yeccpars2_236(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 NewStack = yeccpars2_236_(Stack),
 yeccpars2_251(251, Cat, [236 | Ss], NewStack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccpars2_237/7}).
yeccpars2_237(S, by, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 238, Ss, Stack, T, Ts, Tzr);
yeccpars2_237(_, _, _, _, T, _, _) ->
 yeccerror(T).

yeccpars2_238(S, name, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 244, Ss, Stack, T, Ts, Tzr);
yeccpars2_238(S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_122(S, Cat, Ss, Stack, T, Ts, Tzr).

yeccpars2_239(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 NewStack = yeccpars2_239_(Stack),
 yeccgoto_sort_key(hd(Ss), Cat, Ss, NewStack, T, Ts, Tzr).

yeccpars2_240(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_|Nss] = Ss,
 NewStack = yeccpars2_240_(Stack),
 yeccgoto_opt_order_by_clause(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_241(S, ',', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 249, Ss, Stack, T, Ts, Tzr);
yeccpars2_241(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 NewStack = yeccpars2_241_(Stack),
 yeccgoto_sort_spec_commalist(hd(Ss), Cat, Ss, NewStack, T, Ts, Tzr).

yeccpars2_242(S, asc, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 247, Ss, Stack, T, Ts, Tzr);
yeccpars2_242(S, desc, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 248, Ss, Stack, T, Ts, Tzr);
yeccpars2_242(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 NewStack = yeccpars2_242_(Stack),
 yeccgoto_sort_spec(hd(Ss), Cat, Ss, NewStack, T, Ts, Tzr).

yeccpars2_243(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccgoto_sort_key(hd(Ss), Cat, Ss, Stack, T, Ts, Tzr).

yeccpars2_244(S, '.', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 245, Ss, Stack, T, Ts, Tzr);
yeccpars2_244(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 NewStack = yeccpars2_244_(Stack),
 yeccgoto_table_column(hd(Ss), Cat, Ss, NewStack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccpars2_245/7}).
yeccpars2_245(S, name, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 58, Ss, Stack, T, Ts, Tzr);
yeccpars2_245(_, _, _, _, T, _, _) ->
 yeccerror(T).

yeccpars2_246(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_|Nss] = Ss,
 NewStack = yeccpars2_246_(Stack),
 yeccgoto_sort_spec(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_247(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 NewStack = yeccpars2_247_(Stack),
 yeccgoto_ordering_spec(hd(Ss), Cat, Ss, NewStack, T, Ts, Tzr).

yeccpars2_248(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 NewStack = yeccpars2_248_(Stack),
 yeccgoto_ordering_spec(hd(Ss), Cat, Ss, NewStack, T, Ts, Tzr).

%% yeccpars2_249: see yeccpars2_238

yeccpars2_250(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_|Nss] = Ss,
 NewStack = yeccpars2_250_(Stack),
 yeccgoto_sort_spec_commalist(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_251(S, having, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 262, Ss, Stack, T, Ts, Tzr);
yeccpars2_251(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 NewStack = yeccpars2_251_(Stack),
 yeccpars2_261(261, Cat, [251 | Ss], NewStack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccpars2_252/7}).
yeccpars2_252(S, by, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 253, Ss, Stack, T, Ts, Tzr);
yeccpars2_252(_, _, _, _, T, _, _) ->
 yeccerror(T).

yeccpars2_253(S, name, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 256, Ss, Stack, T, Ts, Tzr);
yeccpars2_253(S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_122(S, Cat, Ss, Stack, T, Ts, Tzr).

yeccpars2_254(S, ',', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 259, Ss, Stack, T, Ts, Tzr);
yeccpars2_254(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 NewStack = yeccpars2_254_(Stack),
 yeccgoto_column_ref_commalist(hd(Ss), Cat, Ss, NewStack, T, Ts, Tzr).

yeccpars2_255(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_|Nss] = Ss,
 NewStack = yeccpars2_255_(Stack),
 yeccgoto_opt_group_by_clause(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_256(S, ',', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 257, Ss, Stack, T, Ts, Tzr);
yeccpars2_256(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 NewStack = yeccpars2_256_(Stack),
 yeccgoto_column_ref_commalist(hd(Ss), Cat, Ss, NewStack, T, Ts, Tzr).

%% yeccpars2_257: see yeccpars2_253

yeccpars2_258(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_|Nss] = Ss,
 NewStack = yeccpars2_258_(Stack),
 yeccgoto_column_ref_commalist(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

%% yeccpars2_259: see yeccpars2_253

yeccpars2_260(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_|Nss] = Ss,
 NewStack = yeccpars2_260_(Stack),
 yeccgoto_column_ref_commalist(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_261(S, limit, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 215, Ss, Stack, T, Ts, Tzr);
yeccpars2_261(S, offset, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 216, Ss, Stack, T, Ts, Tzr);
yeccpars2_261(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 NewStack = yeccpars2_261_(Stack),
 yeccpars2_264(_S, Cat, [261 | Ss], NewStack, T, Ts, Tzr).

%% yeccpars2_262: see yeccpars2_92

yeccpars2_263(S, 'and', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 105, Ss, Stack, T, Ts, Tzr);
yeccpars2_263(S, 'or', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 106, Ss, Stack, T, Ts, Tzr);
yeccpars2_263(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_|Nss] = Ss,
 NewStack = yeccpars2_263_(Stack),
 yeccgoto_opt_having_clause(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_264(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_,_,_|Nss] = Ss,
 NewStack = yeccpars2_264_(Stack),
 yeccgoto_table_exp(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

%% yeccpars2_265: see yeccpars2_16

yeccpars2_266(S, '(', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 270, Ss, Stack, T, Ts, Tzr);
yeccpars2_266(S, name, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 38, Ss, Stack, T, Ts, Tzr);
yeccpars2_266(S, set, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 271, Ss, Stack, T, Ts, Tzr);
yeccpars2_266(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 NewStack = yeccpars2_266_(Stack),
 yeccpars2_267(267, Cat, [266 | Ss], NewStack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccpars2_267/7}).
yeccpars2_267(S, select, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 19, Ss, Stack, T, Ts, Tzr);
yeccpars2_267(S, values, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 282, Ss, Stack, T, Ts, Tzr);
yeccpars2_267(_, _, _, _, T, _, _) ->
 yeccerror(T).

yeccpars2_268(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccgoto_opt_column_commalist(hd(Ss), Cat, Ss, Stack, T, Ts, Tzr).

yeccpars2_269(S, ',', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 278, Ss, Stack, T, Ts, Tzr);
yeccpars2_269(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 NewStack = yeccpars2_269_(Stack),
 yeccgoto_column_commalist(hd(Ss), Cat, Ss, NewStack, T, Ts, Tzr).

%% yeccpars2_270: see yeccpars2_36

%% yeccpars2_271: see yeccpars2_31

yeccpars2_272(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_,_,_|Nss] = Ss,
 NewStack = yeccpars2_272_(Stack),
 yeccgoto_insert_stmt(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_273(S, ',', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 274, Ss, Stack, T, Ts, Tzr);
yeccpars2_273(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 NewStack = yeccpars2_273_(Stack),
 yeccgoto_insert_set_pair_list(hd(Ss), Cat, Ss, NewStack, T, Ts, Tzr).

%% yeccpars2_274: see yeccpars2_31

yeccpars2_275(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_|Nss] = Ss,
 NewStack = yeccpars2_275_(Stack),
 yeccgoto_insert_set_pair_list(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccpars2_276/7}).
yeccpars2_276(S, ')', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 277, Ss, Stack, T, Ts, Tzr);
yeccpars2_276(_, _, _, _, T, _, _) ->
 yeccerror(T).

yeccpars2_277(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_|Nss] = Ss,
 NewStack = yeccpars2_277_(Stack),
 yeccgoto_opt_column_commalist(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

%% yeccpars2_278: see yeccpars2_36

yeccpars2_279(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_|Nss] = Ss,
 NewStack = yeccpars2_279_(Stack),
 yeccgoto_column_commalist(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_280(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_,_,_|Nss] = Ss,
 NewStack = yeccpars2_280_(Stack),
 yeccgoto_insert_stmt(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_281(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccgoto_values_or_select_stmt(hd(Ss), Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccpars2_282/7}).
yeccpars2_282(S, '(', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 285, Ss, Stack, T, Ts, Tzr);
yeccpars2_282(_, _, _, _, T, _, _) ->
 yeccerror(T).

yeccpars2_283(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_|Nss] = Ss,
 NewStack = yeccpars2_283_(Stack),
 yeccgoto_values_or_select_stmt(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_284(S, ',', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 291, Ss, Stack, T, Ts, Tzr);
yeccpars2_284(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 NewStack = yeccpars2_284_(Stack),
 yeccgoto_insert_row_list(hd(Ss), Cat, Ss, NewStack, T, Ts, Tzr).

%% yeccpars2_285: see yeccpars2_39

-dialyzer({nowarn_function, yeccpars2_286/7}).
yeccpars2_286(S, ')', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 290, Ss, Stack, T, Ts, Tzr);
yeccpars2_286(_, _, _, _, T, _, _) ->
 yeccerror(T).

yeccpars2_287(S, '*', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 61, Ss, Stack, T, Ts, Tzr);
yeccpars2_287(S, '+', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 62, Ss, Stack, T, Ts, Tzr);
yeccpars2_287(S, ',', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 288, Ss, Stack, T, Ts, Tzr);
yeccpars2_287(S, '-', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 64, Ss, Stack, T, Ts, Tzr);
yeccpars2_287(S, '/', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 65, Ss, Stack, T, Ts, Tzr);
yeccpars2_287(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 NewStack = yeccpars2_287_(Stack),
 yeccgoto_scalar_exp_list(hd(Ss), Cat, Ss, NewStack, T, Ts, Tzr).

%% yeccpars2_288: see yeccpars2_39

yeccpars2_289(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_|Nss] = Ss,
 NewStack = yeccpars2_289_(Stack),
 yeccgoto_scalar_exp_list(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_290(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_|Nss] = Ss,
 NewStack = yeccpars2_290_(Stack),
 yeccgoto_insert_row(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

%% yeccpars2_291: see yeccpars2_282

yeccpars2_292(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_|Nss] = Ss,
 NewStack = yeccpars2_292_(Stack),
 yeccgoto_insert_row_list(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_293(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_|Nss] = Ss,
 NewStack = yeccpars2_293_(Stack),
 yeccgoto_describe_stmt(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

%% yeccpars2_294: see yeccpars2_16

yeccpars2_295(S, where, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 92, Ss, Stack, T, Ts, Tzr);
yeccpars2_295(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 NewStack = yeccpars2_295_(Stack),
 yeccpars2_296(_S, Cat, [295 | Ss], NewStack, T, Ts, Tzr).

yeccpars2_296(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_,_|Nss] = Ss,
 NewStack = yeccpars2_296_(Stack),
 yeccgoto_delete_stmt(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_297(S, 'begin', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 13, Ss, Stack, T, Ts, Tzr);
yeccpars2_297(S, commit, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 14, Ss, Stack, T, Ts, Tzr);
yeccpars2_297(S, delete, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 15, Ss, Stack, T, Ts, Tzr);
yeccpars2_297(S, describe, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 16, Ss, Stack, T, Ts, Tzr);
yeccpars2_297(S, insert, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 17, Ss, Stack, T, Ts, Tzr);
yeccpars2_297(S, rollback, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 18, Ss, Stack, T, Ts, Tzr);
yeccpars2_297(S, select, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 19, Ss, Stack, T, Ts, Tzr);
yeccpars2_297(S, set, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 20, Ss, Stack, T, Ts, Tzr);
yeccpars2_297(S, show, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 21, Ss, Stack, T, Ts, Tzr);
yeccpars2_297(S, update, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 22, Ss, Stack, T, Ts, Tzr);
yeccpars2_297(S, use, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 23, Ss, Stack, T, Ts, Tzr);
yeccpars2_297(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_|Nss] = Ss,
 NewStack = yeccpars2_297_(Stack),
 yeccgoto_sql_list(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_298(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_|Nss] = Ss,
 NewStack = yeccpars2_298_(Stack),
 yeccgoto_sql_list(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccgoto_assign/7}).
yeccgoto_assign(31, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_34(34, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_assign(88, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_34(34, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_assign(271, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_273(273, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_assign(274, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_273(273, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccgoto_assign_commalist/7}).
yeccgoto_assign_commalist(31, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_33(33, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_assign_commalist(88=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_89(_S, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccgoto_atom/7}).
yeccgoto_atom(19=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_46(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_atom(39=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_46(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_atom(47=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_46(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_atom(55=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_46(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_atom(61=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_46(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_atom(62=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_46(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_atom(63=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_46(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_atom(64=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_46(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_atom(65=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_46(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_atom(72=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_46(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_atom(92=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_46(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_atom(102=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_46(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_atom(103=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_46(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_atom(105=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_46(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_atom(106=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_46(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_atom(111=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_46(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_atom(112=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_46(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_atom(113=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_46(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_atom(114=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_46(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_atom(115=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_46(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_atom(116=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_46(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_atom(117=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_46(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_atom(118=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_46(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_atom(119=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_46(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_atom(120=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_46(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_atom(122=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_139(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_atom(124=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_46(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_atom(126=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_127(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_atom(128, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_131(131, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_atom(132, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_131(131, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_atom(137=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_46(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_atom(140, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_131(131, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_atom(146=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_46(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_atom(158=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_46(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_atom(160=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_46(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_atom(162=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_46(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_atom(195=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_46(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_atom(202=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_46(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_atom(210=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_46(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_atom(262=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_46(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_atom(285=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_46(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_atom(288=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_46(_S, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccgoto_atom_commalist/7}).
yeccgoto_atom_commalist(128, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_130(130, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_atom_commalist(132=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_133(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_atom_commalist(140, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_142(142, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccgoto_between_pred/7}).
yeccgoto_between_pred(92=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_101(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_between_pred(102=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_101(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_between_pred(103=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_101(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_between_pred(105=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_101(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_between_pred(106=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_101(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_between_pred(262=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_101(_S, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccgoto_cast_expr/7}).
yeccgoto_cast_expr(19=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_45(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_cast_expr(39=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_45(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_cast_expr(47=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_45(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_cast_expr(55=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_45(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_cast_expr(61=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_45(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_cast_expr(62=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_45(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_cast_expr(63=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_45(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_cast_expr(64=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_45(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_cast_expr(65=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_45(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_cast_expr(72=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_45(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_cast_expr(92=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_45(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_cast_expr(102=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_45(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_cast_expr(103=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_45(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_cast_expr(105=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_45(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_cast_expr(106=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_45(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_cast_expr(111=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_45(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_cast_expr(112=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_45(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_cast_expr(113=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_45(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_cast_expr(114=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_45(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_cast_expr(115=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_45(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_cast_expr(116=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_45(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_cast_expr(117=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_45(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_cast_expr(118=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_45(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_cast_expr(119=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_45(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_cast_expr(120=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_45(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_cast_expr(124=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_45(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_cast_expr(137=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_45(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_cast_expr(146=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_45(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_cast_expr(158=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_45(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_cast_expr(160=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_45(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_cast_expr(162=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_45(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_cast_expr(195=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_45(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_cast_expr(202=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_45(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_cast_expr(210=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_45(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_cast_expr(262=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_45(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_cast_expr(285=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_45(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_cast_expr(288=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_45(_S, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccgoto_column/7}).
yeccgoto_column(31, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_32(32, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_column(36, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_37(37, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_column(88, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_32(32, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_column(266, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_269(269, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_column(270, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_269(269, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_column(271, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_32(32, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_column(274, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_32(32, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_column(278, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_269(269, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccgoto_column_commalist/7}).
yeccgoto_column_commalist(266=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_268(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_column_commalist(270, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_276(276, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_column_commalist(278=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_279(_S, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccgoto_column_ref/7}).
yeccgoto_column_ref(19=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_44(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_column_ref(39=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_44(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_column_ref(47=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_44(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_column_ref(55=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_44(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_column_ref(61=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_44(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_column_ref(62=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_44(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_column_ref(63=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_44(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_column_ref(64=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_44(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_column_ref(65=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_44(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_column_ref(72=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_44(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_column_ref(92, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_100(100, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_column_ref(102, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_100(100, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_column_ref(103, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_100(100, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_column_ref(105, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_100(100, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_column_ref(106, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_100(100, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_column_ref(111=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_44(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_column_ref(112=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_44(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_column_ref(113=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_44(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_column_ref(114=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_44(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_column_ref(115=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_44(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_column_ref(116=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_44(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_column_ref(117=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_44(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_column_ref(118=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_44(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_column_ref(119=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_44(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_column_ref(120=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_44(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_column_ref(124=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_44(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_column_ref(137=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_44(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_column_ref(146=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_44(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_column_ref(158=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_44(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_column_ref(160=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_44(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_column_ref(162=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_44(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_column_ref(195=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_44(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_column_ref(202=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_44(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_column_ref(210=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_44(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_column_ref(262, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_100(100, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_column_ref(285=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_44(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_column_ref(288=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_44(_S, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccgoto_column_ref_commalist/7}).
yeccgoto_column_ref_commalist(253=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_255(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_column_ref_commalist(257=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_258(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_column_ref_commalist(259=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_260(_S, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccgoto_comparsion_pred/7}).
yeccgoto_comparsion_pred(92=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_99(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_comparsion_pred(102=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_99(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_comparsion_pred(103=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_99(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_comparsion_pred(105=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_99(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_comparsion_pred(106=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_99(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_comparsion_pred(262=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_99(_S, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccgoto_delete_stmt/7}).
yeccgoto_delete_stmt(0=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_12(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_delete_stmt(297=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_12(_S, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccgoto_describe_stmt/7}).
yeccgoto_describe_stmt(0=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_11(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_describe_stmt(297=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_11(_S, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccgoto_from_clause/7}).
yeccgoto_from_clause(198, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_213(213, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccgoto_in_pred/7}).
yeccgoto_in_pred(92=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_98(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_in_pred(102=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_98(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_in_pred(103=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_98(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_in_pred(105=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_98(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_in_pred(106=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_98(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_in_pred(262=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_98(_S, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccgoto_insert_row/7}).
yeccgoto_insert_row(282, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_284(284, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_insert_row(291, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_284(284, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccgoto_insert_row_list/7}).
yeccgoto_insert_row_list(282=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_283(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_insert_row_list(291=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_292(_S, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccgoto_insert_set_pair_list/7}).
yeccgoto_insert_set_pair_list(271=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_272(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_insert_set_pair_list(274=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_275(_S, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccgoto_insert_stmt/7}).
yeccgoto_insert_stmt(0=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_10(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_insert_stmt(297=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_10(_S, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccgoto_like_pred/7}).
yeccgoto_like_pred(92=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_97(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_like_pred(102=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_97(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_like_pred(103=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_97(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_like_pred(105=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_97(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_like_pred(106=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_97(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_like_pred(262=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_97(_S, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccgoto_literal/7}).
yeccgoto_literal(19=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_43(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_literal(39=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_43(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_literal(47=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_43(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_literal(55=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_43(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_literal(61=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_43(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_literal(62=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_43(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_literal(63=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_43(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_literal(64=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_43(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_literal(65=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_43(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_literal(72=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_43(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_literal(92=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_43(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_literal(102=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_43(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_literal(103=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_43(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_literal(105=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_43(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_literal(106=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_43(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_literal(111=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_43(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_literal(112=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_43(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_literal(113=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_43(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_literal(114=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_43(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_literal(115=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_43(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_literal(116=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_43(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_literal(117=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_43(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_literal(118=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_43(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_literal(119=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_43(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_literal(120=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_43(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_literal(122=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_43(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_literal(124=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_43(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_literal(126=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_43(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_literal(128=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_43(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_literal(132=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_43(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_literal(137=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_43(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_literal(140=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_43(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_literal(146=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_43(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_literal(158=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_43(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_literal(160=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_43(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_literal(162=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_43(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_literal(184=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_185(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_literal(187=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_194(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_literal(195=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_43(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_literal(202=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_43(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_literal(210=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_43(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_literal(215, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_218(218, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_literal(216=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_217(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_literal(219=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_222(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_literal(220=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_221(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_literal(238=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_243(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_literal(249=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_243(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_literal(253, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_254(254, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_literal(257, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_254(254, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_literal(259, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_254(254, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_literal(262=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_43(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_literal(285=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_43(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_literal(288=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_43(_S, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccgoto_manipulative_stmt/7}).
yeccgoto_manipulative_stmt(0=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_9(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_manipulative_stmt(297=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_9(_S, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccgoto_opt_charset/7}).
yeccgoto_opt_charset(75, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_76(76, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_opt_charset(82, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_83(83, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccgoto_opt_collate/7}).
yeccgoto_opt_collate(201=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_205(_S, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccgoto_opt_column_commalist/7}).
yeccgoto_opt_column_commalist(266, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_267(267, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccgoto_opt_condition/7}).
yeccgoto_opt_condition(179=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_183(_S, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccgoto_opt_from/7}).
yeccgoto_opt_from(169, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_179(179, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccgoto_opt_full/7}).
yeccgoto_opt_full(21, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_167(167, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccgoto_opt_group_by_clause/7}).
yeccgoto_opt_group_by_clause(236, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_251(251, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccgoto_opt_having_clause/7}).
yeccgoto_opt_having_clause(251, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_261(261, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccgoto_opt_limit_offset_clause/7}).
yeccgoto_opt_limit_offset_clause(198=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_212(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_opt_limit_offset_clause(261=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_264(_S, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccgoto_opt_order_by_clause/7}).
yeccgoto_opt_order_by_clause(235, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_236(236, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccgoto_opt_where_clause/7}).
yeccgoto_opt_where_clause(33=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_91(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_opt_where_clause(213, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_235(235, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_opt_where_clause(295=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_296(_S, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccgoto_ordering_spec/7}).
yeccgoto_ordering_spec(242=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_246(_S, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccgoto_predicate/7}).
yeccgoto_predicate(92=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_96(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_predicate(102=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_96(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_predicate(103=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_96(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_predicate(105=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_96(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_predicate(106=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_96(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_predicate(262=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_96(_S, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccgoto_scalar_exp/7}).
yeccgoto_scalar_exp(19, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_201(201, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_scalar_exp(39, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_42(42, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_scalar_exp(47, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_86(86, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_scalar_exp(55, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_60(60, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_scalar_exp(61, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_70(70, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_scalar_exp(62, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_69(69, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_scalar_exp(63, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_60(60, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_scalar_exp(64, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_67(67, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_scalar_exp(65, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_66(66, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_scalar_exp(72, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_73(73, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_scalar_exp(92, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_95(95, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_scalar_exp(102, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_110(110, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_scalar_exp(103, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_95(95, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_scalar_exp(105, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_95(95, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_scalar_exp(106, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_95(95, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_scalar_exp(111, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_156(156, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_scalar_exp(112, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_155(155, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_scalar_exp(113, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_154(154, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_scalar_exp(114, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_153(153, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_scalar_exp(115, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_152(152, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_scalar_exp(116, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_151(151, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_scalar_exp(117, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_150(150, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_scalar_exp(118, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_149(149, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_scalar_exp(119, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_148(148, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_scalar_exp(120, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_145(145, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_scalar_exp(124, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_136(136, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_scalar_exp(137, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_138(138, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_scalar_exp(146, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_147(147, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_scalar_exp(158, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_159(159, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_scalar_exp(160, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_161(161, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_scalar_exp(162, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_163(163, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_scalar_exp(195, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_197(197, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_scalar_exp(202, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_86(86, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_scalar_exp(210, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_201(201, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_scalar_exp(262, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_95(95, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_scalar_exp(285, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_287(287, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_scalar_exp(288, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_287(287, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccgoto_scalar_exp_commalist/7}).
yeccgoto_scalar_exp_commalist(55, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_59(59, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_scalar_exp_commalist(63=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_68(_S, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccgoto_scalar_exp_list/7}).
yeccgoto_scalar_exp_list(285, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_286(286, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_scalar_exp_list(288=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_289(_S, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccgoto_search_cond/7}).
yeccgoto_search_cond(92, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_94(94, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_search_cond(102, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_109(109, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_search_cond(103, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_104(104, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_search_cond(105, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_108(108, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_search_cond(106, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_107(107, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_search_cond(262, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_263(263, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccgoto_select_stmt/7}).
yeccgoto_select_stmt(0=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_8(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_select_stmt(128, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_129(129, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_select_stmt(140, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_141(141, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_select_stmt(202, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_203(203, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_select_stmt(227, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_228(228, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_select_stmt(267=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_281(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_select_stmt(297=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_8(_S, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccgoto_selection_item/7}).
yeccgoto_selection_item(19, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_200(200, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_selection_item(210, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_200(200, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccgoto_selection_item_alias/7}).
yeccgoto_selection_item_alias(19, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_199(199, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_selection_item_alias(210, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_199(199, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccgoto_selection_list/7}).
yeccgoto_selection_list(19, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_198(198, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_selection_list(210=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_211(_S, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccgoto_set_assign/7}).
yeccgoto_set_assign(20, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_189(189, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_set_assign(191, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_189(189, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccgoto_set_assign_list/7}).
yeccgoto_set_assign_list(20=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_188(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_set_assign_list(191=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_192(_S, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccgoto_set_stmt/7}).
yeccgoto_set_stmt(0=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_7(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_set_stmt(297=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_7(_S, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccgoto_set_target/7}).
yeccgoto_set_target(20, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_187(187, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_set_target(191, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_187(187, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccgoto_set_value/7}).
yeccgoto_set_value(187=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_193(_S, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccgoto_show_stmt/7}).
yeccgoto_show_stmt(0=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_6(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_show_stmt(297=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_6(_S, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccgoto_show_type/7}).
yeccgoto_show_type(167, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_169(169, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccgoto_sort_key/7}).
yeccgoto_sort_key(238, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_242(242, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_sort_key(249, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_242(242, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccgoto_sort_spec/7}).
yeccgoto_sort_spec(238, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_241(241, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_sort_spec(249, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_241(241, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccgoto_sort_spec_commalist/7}).
yeccgoto_sort_spec_commalist(238=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_240(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_sort_spec_commalist(249=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_250(_S, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccgoto_sql/7}).
yeccgoto_sql(0, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_5(5, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_sql(297, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_5(5, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccgoto_sql_list/7}).
yeccgoto_sql_list(0, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_4(4, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_sql_list(297=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_298(_S, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccgoto_table_column/7}).
yeccgoto_table_column(19=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_41(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_table_column(39=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_41(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_table_column(47=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_41(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_table_column(55=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_41(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_table_column(61=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_41(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_table_column(62=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_41(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_table_column(63=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_41(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_table_column(64=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_41(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_table_column(65=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_41(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_table_column(72=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_41(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_table_column(92=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_41(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_table_column(102=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_41(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_table_column(103=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_41(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_table_column(105=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_41(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_table_column(106=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_41(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_table_column(111=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_41(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_table_column(112=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_41(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_table_column(113=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_41(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_table_column(114=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_41(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_table_column(115=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_41(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_table_column(116=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_41(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_table_column(117=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_41(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_table_column(118=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_41(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_table_column(119=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_41(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_table_column(120=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_41(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_table_column(124=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_41(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_table_column(137=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_41(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_table_column(146=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_41(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_table_column(158=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_41(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_table_column(160=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_41(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_table_column(162=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_41(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_table_column(195=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_41(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_table_column(202=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_41(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_table_column(210=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_41(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_table_column(238=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_239(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_table_column(249=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_239(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_table_column(262=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_41(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_table_column(285=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_41(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_table_column(288=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_41(_S, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccgoto_table_exp/7}).
yeccgoto_table_exp(213=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_234(_S, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccgoto_table_name/7}).
yeccgoto_table_name(16=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_293(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_table_name(22, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_26(26, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_table_name(214=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_226(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_table_name(232=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_226(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_table_name(265, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_266(266, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_table_name(294, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_295(295, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccgoto_table_ref/7}).
yeccgoto_table_ref(214, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_225(225, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_table_ref(232, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_225(225, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccgoto_table_ref_alias/7}).
yeccgoto_table_ref_alias(214, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_224(224, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_table_ref_alias(232, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_224(224, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccgoto_table_ref_commalist/7}).
yeccgoto_table_ref_commalist(214=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_223(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_table_ref_commalist(232=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_233(_S, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccgoto_test_for_null_pred/7}).
yeccgoto_test_for_null_pred(92=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_93(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_test_for_null_pred(102=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_93(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_test_for_null_pred(103=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_93(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_test_for_null_pred(105=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_93(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_test_for_null_pred(106=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_93(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_test_for_null_pred(262=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_93(_S, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccgoto_tname/7}).
yeccgoto_tname(16, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_25(25, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_tname(22, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_25(25, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_tname(214, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_25(25, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_tname(232, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_25(25, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_tname(265, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_25(25, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_tname(294, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_25(25, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccgoto_transaction_stmt/7}).
yeccgoto_transaction_stmt(0=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_3(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_transaction_stmt(297=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_3(_S, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccgoto_update_stmt/7}).
yeccgoto_update_stmt(0=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_2(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_update_stmt(297=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_2(_S, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccgoto_use_stmt/7}).
yeccgoto_use_stmt(0=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_1(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_use_stmt(297=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_1(_S, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccgoto_values_or_select_stmt/7}).
yeccgoto_values_or_select_stmt(267=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_280(_S, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccgoto_variable/7}).
yeccgoto_variable(19=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_40(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_variable(20=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_186(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_variable(39=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_40(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_variable(47=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_40(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_variable(55=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_40(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_variable(61=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_40(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_variable(62=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_40(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_variable(63=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_40(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_variable(64=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_40(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_variable(65=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_40(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_variable(72=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_40(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_variable(92=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_40(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_variable(102=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_40(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_variable(103=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_40(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_variable(105=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_40(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_variable(106=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_40(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_variable(111=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_40(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_variable(112=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_40(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_variable(113=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_40(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_variable(114=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_40(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_variable(115=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_40(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_variable(116=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_40(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_variable(117=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_40(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_variable(118=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_40(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_variable(119=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_40(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_variable(120=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_40(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_variable(124=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_40(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_variable(137=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_40(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_variable(146=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_40(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_variable(158=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_40(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_variable(160=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_40(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_variable(162=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_40(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_variable(191=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_186(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_variable(195=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_40(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_variable(202=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_40(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_variable(210=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_40(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_variable(262=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_40(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_variable(285=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_40(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_variable(288=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_40(_S, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccgoto_where_clause/7}).
yeccgoto_where_clause(33=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_90(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_where_clause(179=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_182(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_where_clause(213=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_90(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_where_clause(295=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_90(_S, Cat, Ss, Stack, T, Ts, Tzr).

-compile({inline,yeccpars2_5_/1}).
-file("src/sql92_parser.yrl", 30).
yeccpars2_5_(__Stack0) ->
 [__1 | __Stack] = __Stack0,
 [begin
   [ __1 ]
  end | __Stack].

-compile({inline,yeccpars2_13_/1}).
-file("src/sql92_parser.yrl", 46).
yeccpars2_13_(__Stack0) ->
 [__1 | __Stack] = __Stack0,
 [begin
   'begin'
  end | __Stack].

-compile({inline,yeccpars2_14_/1}).
-file("src/sql92_parser.yrl", 48).
yeccpars2_14_(__Stack0) ->
 [__1 | __Stack] = __Stack0,
 [begin
   commit
  end | __Stack].

-compile({inline,yeccpars2_18_/1}).
-file("src/sql92_parser.yrl", 47).
yeccpars2_18_(__Stack0) ->
 [__1 | __Stack] = __Stack0,
 [begin
   rollback
  end | __Stack].

-compile({inline,yeccpars2_21_/1}).
-file("src/sql92_parser.yrl", 109).
yeccpars2_21_(__Stack0) ->
 [begin
   false
  end | __Stack0].

-compile({inline,yeccpars2_24_/1}).
-file("src/sql92_parser.yrl", 126).
yeccpars2_24_(__Stack0) ->
 [__2,__1 | __Stack] = __Stack0,
 [begin
   { use , value_of ( __2 ) }
  end | __Stack].

-compile({inline,yeccpars2_25_/1}).
-file("src/sql92_parser.yrl", 145).
yeccpars2_25_(__Stack0) ->
 [__1 | __Stack] = __Stack0,
 [begin
   # table { name = value_of ( __1 ) , alias = case value_of ( __1 ) of { _ , A } -> A ; A -> A end }
  end | __Stack].

-compile({inline,yeccpars2_27_/1}).
-file("src/sql92_parser.yrl", 141).
yeccpars2_27_(__Stack0) ->
 [__1 | __Stack] = __Stack0,
 [begin
   { element ( 1 , __1 ) , element ( 2 , __1 ) , tolower ( value_of ( __1 ) ) }
  end | __Stack].

-compile({inline,yeccpars2_29_/1}).
-file("src/sql92_parser.yrl", 139).
yeccpars2_29_(__Stack0) ->
 [__3,__2,__1 | __Stack] = __Stack0,
 [begin
   { element ( 1 , __1 ) , element ( 2 , __1 ) , { tolower ( value_of ( __1 ) ) , tolower ( value_of ( __3 ) ) } }
  end | __Stack].

-compile({inline,yeccpars2_30_/1}).
-file("src/sql92_parser.yrl", 140).
yeccpars2_30_(__Stack0) ->
 [__3,__2,__1 | __Stack] = __Stack0,
 [begin
   { element ( 1 , __1 ) , element ( 2 , __1 ) , { tolower ( value_of ( __1 ) ) , << "tables" >> } }
  end | __Stack].

-compile({inline,yeccpars2_33_/1}).
-file("src/sql92_parser.yrl", 176).
yeccpars2_33_(__Stack0) ->
 [begin
   undefined
  end | __Stack0].

-compile({inline,yeccpars2_34_/1}).
-file("src/sql92_parser.yrl", 133).
yeccpars2_34_(__Stack0) ->
 [__1 | __Stack] = __Stack0,
 [begin
   [ __1 ]
  end | __Stack].

-compile({inline,yeccpars2_35_/1}).
-file("src/sql92_parser.yrl", 288).
yeccpars2_35_(__Stack0) ->
 [__1 | __Stack] = __Stack0,
 [begin
   value_of ( __1 )
  end | __Stack].

-compile({inline,yeccpars2_38_/1}).
-file("src/sql92_parser.yrl", 288).
yeccpars2_38_(__Stack0) ->
 [__1 | __Stack] = __Stack0,
 [begin
   value_of ( __1 )
  end | __Stack].

-compile({inline,yeccpars2_42_/1}).
-file("src/sql92_parser.yrl", 135).
yeccpars2_42_(__Stack0) ->
 [__5,__4,__3,__2,__1 | __Stack] = __Stack0,
 [begin
   # set { key = __3 , value = __5 }
  end | __Stack].

-compile({inline,yeccpars2_46_/1}).
-file("src/sql92_parser.yrl", 271).
yeccpars2_46_(__Stack0) ->
 [__1 | __Stack] = __Stack0,
 [begin
   # value { value = __1 }
  end | __Stack].

-compile({inline,yeccpars2_48_/1}).
-file("src/sql92_parser.yrl", 268).
yeccpars2_48_(__Stack0) ->
 [__1 | __Stack] = __Stack0,
 [begin
   # all { }
  end | __Stack].

-compile({inline,yeccpars2_50_/1}).
-file("src/sql92_parser.yrl", 289).
yeccpars2_50_(__Stack0) ->
 [__1 | __Stack] = __Stack0,
 [begin
   value_of ( __1 )
  end | __Stack].

-compile({inline,yeccpars2_51_/1}).
-file("src/sql92_parser.yrl", 154).
yeccpars2_51_(__Stack0) ->
 [__1 | __Stack] = __Stack0,
 [begin
   # key { name = value_of ( __1 ) , alias = value_of ( __1 ) }
  end | __Stack].

-compile({inline,yeccpars2_52_/1}).
-file("src/sql92_parser.yrl", 272).
yeccpars2_52_(__Stack0) ->
 [__1 | __Stack] = __Stack0,
 [begin
   # value { value = undefined }
  end | __Stack].

-compile({inline,yeccpars2_53_/1}).
-file("src/sql92_parser.yrl", 290).
yeccpars2_53_(__Stack0) ->
 [__1 | __Stack] = __Stack0,
 [begin
   value_of ( __1 )
  end | __Stack].

-compile({inline,yeccpars2_54_/1}).
-file("src/sql92_parser.yrl", 275).
yeccpars2_54_(__Stack0) ->
 [__1 | __Stack] = __Stack0,
 [begin
   # variable { name = value_of ( __1 ) , scope = local }
  end | __Stack].

-compile({inline,yeccpars2_55_/1}).
-file("src/sql92_parser.yrl", 285).
yeccpars2_55_(__Stack0) ->
 [begin
   [ ]
  end | __Stack0].

-compile({inline,yeccpars2_57_/1}).
-file("src/sql92_parser.yrl", 150).
yeccpars2_57_(__Stack0) ->
 [__3,__2,__1 | __Stack] = __Stack0,
 [begin
   # all { table = value_of ( __1 ) }
  end | __Stack].

-compile({inline,yeccpars2_58_/1}).
-file("src/sql92_parser.yrl", 153).
yeccpars2_58_(__Stack0) ->
 [__3,__2,__1 | __Stack] = __Stack0,
 [begin
   # key { name = value_of ( __3 ) , alias = value_of ( __3 ) , table = value_of ( __1 ) }
  end | __Stack].

-compile({inline,yeccpars2_60_/1}).
-file("src/sql92_parser.yrl", 284).
yeccpars2_60_(__Stack0) ->
 [__1 | __Stack] = __Stack0,
 [begin
   [ __1 ]
  end | __Stack].

-compile({inline,yeccpars2_63_/1}).
-file("src/sql92_parser.yrl", 285).
yeccpars2_63_(__Stack0) ->
 [begin
   [ ]
  end | __Stack0].

-compile({inline,yeccpars2_66_/1}).
-file("src/sql92_parser.yrl", 266).
yeccpars2_66_(__Stack0) ->
 [__3,__2,__1 | __Stack] = __Stack0,
 [begin
   # operation { type = << "/" >> , op1 = __1 , op2 = __3 }
  end | __Stack].

-compile({inline,yeccpars2_67_/1}).
-file("src/sql92_parser.yrl", 264).
yeccpars2_67_(__Stack0) ->
 [__3,__2,__1 | __Stack] = __Stack0,
 [begin
   # operation { type = << "-" >> , op1 = __1 , op2 = __3 }
  end | __Stack].

-compile({inline,yeccpars2_68_/1}).
-file("src/sql92_parser.yrl", 283).
yeccpars2_68_(__Stack0) ->
 [__3,__2,__1 | __Stack] = __Stack0,
 [begin
   [ __1 | __3 ]
  end | __Stack].

-compile({inline,yeccpars2_69_/1}).
-file("src/sql92_parser.yrl", 263).
yeccpars2_69_(__Stack0) ->
 [__3,__2,__1 | __Stack] = __Stack0,
 [begin
   # operation { type = << "+" >> , op1 = __1 , op2 = __3 }
  end | __Stack].

-compile({inline,yeccpars2_70_/1}).
-file("src/sql92_parser.yrl", 265).
yeccpars2_70_(__Stack0) ->
 [__3,__2,__1 | __Stack] = __Stack0,
 [begin
   # operation { type = << "*" >> , op1 = __1 , op2 = __3 }
  end | __Stack].

-compile({inline,yeccpars2_71_/1}).
-file("src/sql92_parser.yrl", 262).
yeccpars2_71_(__Stack0) ->
 [__4,__3,__2,__1 | __Stack] = __Stack0,
 [begin
   # function { name = value_of ( __1 ) , params = __3 }
  end | __Stack].

-compile({inline,yeccpars2_75_/1}).
-file("src/sql92_parser.yrl", 281).
yeccpars2_75_(__Stack0) ->
 [begin
   undefined
  end | __Stack0].

-compile({inline,yeccpars2_80_/1}).
-file("src/sql92_parser.yrl", 280).
yeccpars2_80_(__Stack0) ->
 [__3,__2,__1 | __Stack] = __Stack0,
 [begin
   value_of ( __3 )
  end | __Stack].

-compile({inline,yeccpars2_82_/1}).
-file("src/sql92_parser.yrl", 281).
yeccpars2_82_(__Stack0) ->
 [begin
   undefined
  end | __Stack0].

-compile({inline,yeccpars2_84_/1}).
-file("src/sql92_parser.yrl", 277).
yeccpars2_84_(__Stack0) ->
 [__10,__9,__8,__7,__6,__5,__4,__3,__2,__1 | __Stack] = __Stack0,
 [begin
   # function { name = << "cast" >> , params = [ __3 , { value_of ( __5 ) , value_of ( __7 ) } ] }
  end | __Stack].

-compile({inline,yeccpars2_85_/1}).
-file("src/sql92_parser.yrl", 278).
yeccpars2_85_(__Stack0) ->
 [__7,__6,__5,__4,__3,__2,__1 | __Stack] = __Stack0,
 [begin
   # function { name = << "cast" >> , params = [ __3 , value_of ( __5 ) ] }
  end | __Stack].

-compile({inline,yeccpars2_87_/1}).
-file("src/sql92_parser.yrl", 267).
yeccpars2_87_(__Stack0) ->
 [__3,__2,__1 | __Stack] = __Stack0,
 [begin
   __2
  end | __Stack].

-compile({inline,yeccpars2_89_/1}).
-file("src/sql92_parser.yrl", 132).
yeccpars2_89_(__Stack0) ->
 [__3,__2,__1 | __Stack] = __Stack0,
 [begin
   [ __1 | __3 ]
  end | __Stack].

-compile({inline,yeccpars2_91_/1}).
-file("src/sql92_parser.yrl", 130).
yeccpars2_91_(__Stack0) ->
 [__5,__4,__3,__2,__1 | __Stack] = __Stack0,
 [begin
   { update , __2 , __4 , __5 }
  end | __Stack].

-compile({inline,yeccpars2_94_/1}).
-file("src/sql92_parser.yrl", 179).
yeccpars2_94_(__Stack0) ->
 [__2,__1 | __Stack] = __Stack0,
 [begin
   __2
  end | __Stack].

-compile({inline,yeccpars2_104_/1}).
-file("src/sql92_parser.yrl", 221).
yeccpars2_104_(__Stack0) ->
 [__2,__1 | __Stack] = __Stack0,
 [begin
   { 'not' , __2 }
  end | __Stack].

-compile({inline,yeccpars2_107_/1}).
-file("src/sql92_parser.yrl", 219).
yeccpars2_107_(__Stack0) ->
 [__3,__2,__1 | __Stack] = __Stack0,
 [begin
   # condition { nexo = nexo_or , op1 = __1 , op2 = __3 }
  end | __Stack].

-compile({inline,yeccpars2_108_/1}).
-file("src/sql92_parser.yrl", 220).
yeccpars2_108_(__Stack0) ->
 [__3,__2,__1 | __Stack] = __Stack0,
 [begin
   # condition { nexo = nexo_and , op1 = __1 , op2 = __3 }
  end | __Stack].

-compile({inline,yeccpars2_127_/1}).
-file("src/sql92_parser.yrl", 246).
yeccpars2_127_(__Stack0) ->
 [__4,__3,__2,__1 | __Stack] = __Stack0,
 [begin
   # condition { nexo = not_like , op1 = __1 , op2 = __4 }
  end | __Stack].

-compile({inline,yeccpars2_131_/1}).
-file("src/sql92_parser.yrl", 258).
yeccpars2_131_(__Stack0) ->
 [__1 | __Stack] = __Stack0,
 [begin
   [ __1 ]
  end | __Stack].

-compile({inline,yeccpars2_133_/1}).
-file("src/sql92_parser.yrl", 257).
yeccpars2_133_(__Stack0) ->
 [__3,__2,__1 | __Stack] = __Stack0,
 [begin
   [ __1 | __3 ]
  end | __Stack].

-compile({inline,yeccpars2_134_/1}).
-file("src/sql92_parser.yrl", 254).
yeccpars2_134_(__Stack0) ->
 [__6,__5,__4,__3,__2,__1 | __Stack] = __Stack0,
 [begin
   # condition { nexo = not_in , op1 = __1 , op2 = # subquery { subquery = __5 } }
  end | __Stack].

-compile({inline,yeccpars2_135_/1}).
-file("src/sql92_parser.yrl", 252).
yeccpars2_135_(__Stack0) ->
 [__6,__5,__4,__3,__2,__1 | __Stack] = __Stack0,
 [begin
   # condition { nexo = not_in , op1 = __1 , op2 = # subquery { subquery = __5 } }
  end | __Stack].

-compile({inline,yeccpars2_138_/1}).
-file("src/sql92_parser.yrl", 243).
yeccpars2_138_(__Stack0) ->
 [__6,__5,__4,__3,__2,__1 | __Stack] = __Stack0,
 [begin
   # condition { nexo = not_between , op1 = __1 , op2 = { __4 , __6 } }
  end | __Stack].

-compile({inline,yeccpars2_139_/1}).
-file("src/sql92_parser.yrl", 247).
yeccpars2_139_(__Stack0) ->
 [__3,__2,__1 | __Stack] = __Stack0,
 [begin
   # condition { nexo = like , op1 = __1 , op2 = __3 }
  end | __Stack].

-compile({inline,yeccpars2_143_/1}).
-file("src/sql92_parser.yrl", 255).
yeccpars2_143_(__Stack0) ->
 [__5,__4,__3,__2,__1 | __Stack] = __Stack0,
 [begin
   # condition { nexo = in , op1 = __1 , op2 = # subquery { subquery = __4 } }
  end | __Stack].

-compile({inline,yeccpars2_144_/1}).
-file("src/sql92_parser.yrl", 253).
yeccpars2_144_(__Stack0) ->
 [__5,__4,__3,__2,__1 | __Stack] = __Stack0,
 [begin
   # condition { nexo = in , op1 = __1 , op2 = # subquery { subquery = __4 } }
  end | __Stack].

-compile({inline,yeccpars2_147_/1}).
-file("src/sql92_parser.yrl", 244).
yeccpars2_147_(__Stack0) ->
 [__5,__4,__3,__2,__1 | __Stack] = __Stack0,
 [begin
   # condition { nexo = between , op1 = __1 , op2 = { __3 , __5 } }
  end | __Stack].

-compile({inline,yeccpars2_148_/1}).
-file("src/sql92_parser.yrl", 236).
yeccpars2_148_(__Stack0) ->
 [__3,__2,__1 | __Stack] = __Stack0,
 [begin
   # condition { nexo = gte , op1 = __1 , op2 = __3 }
  end | __Stack].

-compile({inline,yeccpars2_149_/1}).
-file("src/sql92_parser.yrl", 234).
yeccpars2_149_(__Stack0) ->
 [__3,__2,__1 | __Stack] = __Stack0,
 [begin
   # condition { nexo = gt , op1 = __1 , op2 = __3 }
  end | __Stack].

-compile({inline,yeccpars2_150_/1}).
-file("src/sql92_parser.yrl", 237).
yeccpars2_150_(__Stack0) ->
 [__3,__2,__1 | __Stack] = __Stack0,
 [begin
   # condition { nexo = gte , op1 = __1 , op2 = __3 }
  end | __Stack].

-compile({inline,yeccpars2_151_/1}).
-file("src/sql92_parser.yrl", 239).
yeccpars2_151_(__Stack0) ->
 [__3,__2,__1 | __Stack] = __Stack0,
 [begin
   # condition { nexo = lte , op1 = __1 , op2 = __3 }
  end | __Stack].

-compile({inline,yeccpars2_152_/1}).
-file("src/sql92_parser.yrl", 233).
yeccpars2_152_(__Stack0) ->
 [__3,__2,__1 | __Stack] = __Stack0,
 [begin
   # condition { nexo = eq , op1 = __1 , op2 = __3 }
  end | __Stack].

-compile({inline,yeccpars2_153_/1}).
-file("src/sql92_parser.yrl", 241).
yeccpars2_153_(__Stack0) ->
 [__3,__2,__1 | __Stack] = __Stack0,
 [begin
   # condition { nexo = neq , op1 = __1 , op2 = __3 }
  end | __Stack].

-compile({inline,yeccpars2_154_/1}).
-file("src/sql92_parser.yrl", 238).
yeccpars2_154_(__Stack0) ->
 [__3,__2,__1 | __Stack] = __Stack0,
 [begin
   # condition { nexo = lte , op1 = __1 , op2 = __3 }
  end | __Stack].

-compile({inline,yeccpars2_155_/1}).
-file("src/sql92_parser.yrl", 235).
yeccpars2_155_(__Stack0) ->
 [__3,__2,__1 | __Stack] = __Stack0,
 [begin
   # condition { nexo = lt , op1 = __1 , op2 = __3 }
  end | __Stack].

-compile({inline,yeccpars2_156_/1}).
-file("src/sql92_parser.yrl", 240).
yeccpars2_156_(__Stack0) ->
 [__3,__2,__1 | __Stack] = __Stack0,
 [begin
   # condition { nexo = neq , op1 = __1 , op2 = __3 }
  end | __Stack].

-compile({inline,yeccpars2_157_/1}).
-file("src/sql92_parser.yrl", 222).
yeccpars2_157_(__Stack0) ->
 [__3,__2,__1 | __Stack] = __Stack0,
 [begin
   __2
  end | __Stack].

-compile({inline,yeccpars2_159_/1}).
-file("src/sql92_parser.yrl", 250).
yeccpars2_159_(__Stack0) ->
 [__3,__2,__1 | __Stack] = __Stack0,
 [begin
   # condition { nexo = is , op1 = __1 , op2 = __3 }
  end | __Stack].

-compile({inline,yeccpars2_161_/1}).
-file("src/sql92_parser.yrl", 249).
yeccpars2_161_(__Stack0) ->
 [__4,__3,__2,__1 | __Stack] = __Stack0,
 [begin
   # condition { nexo = is_not , op1 = __1 , op2 = __4 }
  end | __Stack].

-compile({inline,yeccpars2_163_/1}).
-file("src/sql92_parser.yrl", 136).
yeccpars2_163_(__Stack0) ->
 [__3,__2,__1 | __Stack] = __Stack0,
 [begin
   # set { key = __1 , value = __3 }
  end | __Stack].

-compile({inline,yeccpars2_165_/1}).
-file("src/sql92_parser.yrl", 144).
yeccpars2_165_(__Stack0) ->
 [__2,__1 | __Stack] = __Stack0,
 [begin
   # table { name = value_of ( __1 ) , alias = value_of ( __2 ) }
  end | __Stack].

-compile({inline,yeccpars2_166_/1}).
-file("src/sql92_parser.yrl", 143).
yeccpars2_166_(__Stack0) ->
 [__3,__2,__1 | __Stack] = __Stack0,
 [begin
   # table { name = value_of ( __1 ) , alias = value_of ( __3 ) }
  end | __Stack].

-compile({inline,yeccpars2_168_/1}).
-file("src/sql92_parser.yrl", 108).
yeccpars2_168_(__Stack0) ->
 [__1 | __Stack] = __Stack0,
 [begin
   true
  end | __Stack].

-compile({inline,yeccpars2_169_/1}).
-file("src/sql92_parser.yrl", 111).
yeccpars2_169_(__Stack0) ->
 [begin
   undefined
  end | __Stack0].

-compile({inline,yeccpars2_170_/1}).
-file("src/sql92_parser.yrl", 107).
yeccpars2_170_(__Stack0) ->
 [__1 | __Stack] = __Stack0,
 [begin
   collation
  end | __Stack].

-compile({inline,yeccpars2_172_/1}).
-file("src/sql92_parser.yrl", 105).
yeccpars2_172_(__Stack0) ->
 [__1 | __Stack] = __Stack0,
 [begin
   databases
  end | __Stack].

-compile({inline,yeccpars2_173_/1}).
-file("src/sql92_parser.yrl", 103).
yeccpars2_173_(__Stack0) ->
 [__1 | __Stack] = __Stack0,
 [begin
   fields
  end | __Stack].

-compile({inline,yeccpars2_174_/1}).
-file("src/sql92_parser.yrl", 104).
yeccpars2_174_(__Stack0) ->
 [__1 | __Stack] = __Stack0,
 [begin
   index
  end | __Stack].

-compile({inline,yeccpars2_175_/1}).
-file("src/sql92_parser.yrl", 102).
yeccpars2_175_(__Stack0) ->
 [__1 | __Stack] = __Stack0,
 [begin
   tables
  end | __Stack].

-compile({inline,yeccpars2_176_/1}).
-file("src/sql92_parser.yrl", 106).
yeccpars2_176_(__Stack0) ->
 [__1 | __Stack] = __Stack0,
 [begin
   variables
  end | __Stack].

-compile({inline,yeccpars2_178_/1}).
-file("src/sql92_parser.yrl", 100).
yeccpars2_178_(__Stack0) ->
 [__5,__4,__3,__2,__1 | __Stack] = __Stack0,
 [begin
   # show { type = create_table , full = __2 , from = value_of ( __5 ) }
  end | __Stack].

-compile({inline,yeccpars2_179_/1}).
-file("src/sql92_parser.yrl", 114).
yeccpars2_179_(__Stack0) ->
 [begin
   undefined
  end | __Stack0].

-compile({inline,yeccpars2_181_/1}).
-file("src/sql92_parser.yrl", 110).
yeccpars2_181_(__Stack0) ->
 [__2,__1 | __Stack] = __Stack0,
 [begin
   value_of ( __2 )
  end | __Stack].

-compile({inline,yeccpars2_183_/1}).
-file("src/sql92_parser.yrl", 101).
yeccpars2_183_(__Stack0) ->
 [__5,__4,__3,__2,__1 | __Stack] = __Stack0,
 [begin
   # show { type = __3 , full = __2 , from = __4 , conditions = __5 }
  end | __Stack].

-compile({inline,yeccpars2_185_/1}).
-file("src/sql92_parser.yrl", 112).
yeccpars2_185_(__Stack0) ->
 [__2,__1 | __Stack] = __Stack0,
 [begin
   { like , __2 }
  end | __Stack].

-compile({inline,yeccpars2_188_/1}).
-file("src/sql92_parser.yrl", 116).
yeccpars2_188_(__Stack0) ->
 [__2,__1 | __Stack] = __Stack0,
 [begin
   # system_set { query = __2 }
  end | __Stack].

-compile({inline,yeccpars2_189_/1}).
-file("src/sql92_parser.yrl", 118).
yeccpars2_189_(__Stack0) ->
 [__1 | __Stack] = __Stack0,
 [begin
   [ __1 ]
  end | __Stack].

-compile({inline,yeccpars2_190_/1}).
-file("src/sql92_parser.yrl", 122).
yeccpars2_190_(__Stack0) ->
 [__1 | __Stack] = __Stack0,
 [begin
   # variable { name = value_of ( __1 ) , scope = session }
  end | __Stack].

-compile({inline,yeccpars2_192_/1}).
-file("src/sql92_parser.yrl", 117).
yeccpars2_192_(__Stack0) ->
 [__3,__2,__1 | __Stack] = __Stack0,
 [begin
   [ __1 | __3 ]
  end | __Stack].

-compile({inline,yeccpars2_193_/1}).
-file("src/sql92_parser.yrl", 120).
yeccpars2_193_(__Stack0) ->
 [__2,__1 | __Stack] = __Stack0,
 [begin
   { __1 , __2 }
  end | __Stack].

-compile({inline,yeccpars2_194_/1}).
-file("src/sql92_parser.yrl", 124).
yeccpars2_194_(__Stack0) ->
 [__1 | __Stack] = __Stack0,
 [begin
   # value { value = __1 }
  end | __Stack].

-compile({inline,yeccpars2_196_/1}).
-file("src/sql92_parser.yrl", 123).
yeccpars2_196_(__Stack0) ->
 [__1 | __Stack] = __Stack0,
 [begin
   # value { value = value_of ( __1 ) }
  end | __Stack].

-compile({inline,yeccpars2_197_/1}).
-file("src/sql92_parser.yrl", 119).
yeccpars2_197_(__Stack0) ->
 [__3,__2,__1 | __Stack] = __Stack0,
 [begin
   { __1 , __3 }
  end | __Stack].

-compile({inline,yeccpars2_198_/1}).
-file("src/sql92_parser.yrl", 210).
yeccpars2_198_(__Stack0) ->
 [begin
   # { }
  end | __Stack0].

-compile({inline,yeccpars2_199_/1}).
-file("src/sql92_parser.yrl", 86).
yeccpars2_199_(__Stack0) ->
 [__1 | __Stack] = __Stack0,
 [begin
   [ __1 ]
  end | __Stack].

-compile({inline,yeccpars2_201_/1}).
-file("src/sql92_parser.yrl", 95).
yeccpars2_201_(__Stack0) ->
 [begin
   undefined
  end | __Stack0].

-compile({inline,yeccpars2_204_/1}).
-file("src/sql92_parser.yrl", 91).
yeccpars2_204_(__Stack0) ->
 [__3,__2,__1 | __Stack] = __Stack0,
 [begin
   # subquery { subquery = __2 }
  end | __Stack].

-compile({inline,yeccpars2_205_/1}).
-file("src/sql92_parser.yrl", 92).
yeccpars2_205_(__Stack0) ->
 [__2,__1 | __Stack] = __Stack0,
 [begin
   __1
  end | __Stack].

-compile({inline,yeccpars2_207_/1}).
-file("src/sql92_parser.yrl", 94).
yeccpars2_207_(__Stack0) ->
 [__2,__1 | __Stack] = __Stack0,
 [begin
   { collate , value_of ( __2 ) }
  end | __Stack].

-compile({inline,yeccpars2_209_/1}).
-file("src/sql92_parser.yrl", 88).
yeccpars2_209_(__Stack0) ->
 [__3,__2,__1 | __Stack] = __Stack0,
 [begin
   apply_alias ( __1 , value_of ( __3 ) )
  end | __Stack].

-compile({inline,yeccpars2_211_/1}).
-file("src/sql92_parser.yrl", 85).
yeccpars2_211_(__Stack0) ->
 [__3,__2,__1 | __Stack] = __Stack0,
 [begin
   [ __1 | __3 ]
  end | __Stack].

-compile({inline,yeccpars2_212_/1}).
-file("src/sql92_parser.yrl", 83).
yeccpars2_212_(__Stack0) ->
 [__3,__2,__1 | __Stack] = __Stack0,
 [begin
   # select { params = __2 , limit = map_key ( limit , __3 ) }
  end | __Stack].

-compile({inline,yeccpars2_213_/1}).
-file("src/sql92_parser.yrl", 176).
yeccpars2_213_(__Stack0) ->
 [begin
   undefined
  end | __Stack0].

-compile({inline,yeccpars2_217_/1}).
-file("src/sql92_parser.yrl", 214).
yeccpars2_217_(__Stack0) ->
 [__2,__1 | __Stack] = __Stack0,
 [begin
   # { offset => __2 }
  end | __Stack].

-compile({inline,yeccpars2_218_/1}).
-file("src/sql92_parser.yrl", 213).
yeccpars2_218_(__Stack0) ->
 [__2,__1 | __Stack] = __Stack0,
 [begin
   # { limit => __2 }
  end | __Stack].

-compile({inline,yeccpars2_221_/1}).
-file("src/sql92_parser.yrl", 211).
yeccpars2_221_(__Stack0) ->
 [__4,__3,__2,__1 | __Stack] = __Stack0,
 [begin
   # { limit => __2 , offset => __4 }
  end | __Stack].

-compile({inline,yeccpars2_222_/1}).
-file("src/sql92_parser.yrl", 212).
yeccpars2_222_(__Stack0) ->
 [__4,__3,__2,__1 | __Stack] = __Stack0,
 [begin
   # { offset => __2 , limit => __4 }
  end | __Stack].

-compile({inline,yeccpars2_223_/1}).
-file("src/sql92_parser.yrl", 165).
yeccpars2_223_(__Stack0) ->
 [__2,__1 | __Stack] = __Stack0,
 [begin
   __2
  end | __Stack].

-compile({inline,yeccpars2_224_/1}).
-file("src/sql92_parser.yrl", 168).
yeccpars2_224_(__Stack0) ->
 [__1 | __Stack] = __Stack0,
 [begin
   [ __1 ]
  end | __Stack].

-compile({inline,yeccpars2_229_/1}).
-file("src/sql92_parser.yrl", 173).
yeccpars2_229_(__Stack0) ->
 [__3,__2,__1 | __Stack] = __Stack0,
 [begin
   # subquery { subquery = __2 }
  end | __Stack].

-compile({inline,yeccpars2_231_/1}).
-file("src/sql92_parser.yrl", 170).
yeccpars2_231_(__Stack0) ->
 [__3,__2,__1 | __Stack] = __Stack0,
 [begin
   apply_alias ( __1 , value_of ( __3 ) )
  end | __Stack].

-compile({inline,yeccpars2_233_/1}).
-file("src/sql92_parser.yrl", 167).
yeccpars2_233_(__Stack0) ->
 [__3,__2,__1 | __Stack] = __Stack0,
 [begin
   [ __1 | __3 ]
  end | __Stack].

-compile({inline,yeccpars2_234_/1}).
-file("src/sql92_parser.yrl", 82).
yeccpars2_234_(__Stack0) ->
 [__4,__3,__2,__1 | __Stack] = __Stack0,
 [begin
   __4 # select { params = __2 , tables = __3 }
  end | __Stack].

-compile({inline,yeccpars2_235_/1}).
-file("src/sql92_parser.yrl", 182).
yeccpars2_235_(__Stack0) ->
 [begin
   undefined
  end | __Stack0].

-compile({inline,yeccpars2_236_/1}).
-file("src/sql92_parser.yrl", 199).
yeccpars2_236_(__Stack0) ->
 [begin
   undefined
  end | __Stack0].

-compile({inline,yeccpars2_239_/1}).
-file("src/sql92_parser.yrl", 191).
yeccpars2_239_(__Stack0) ->
 [__1 | __Stack] = __Stack0,
 [begin
   __1 # key .name
  end | __Stack].

-compile({inline,yeccpars2_240_/1}).
-file("src/sql92_parser.yrl", 183).
yeccpars2_240_(__Stack0) ->
 [__3,__2,__1 | __Stack] = __Stack0,
 [begin
   __3
  end | __Stack].

-compile({inline,yeccpars2_241_/1}).
-file("src/sql92_parser.yrl", 186).
yeccpars2_241_(__Stack0) ->
 [__1 | __Stack] = __Stack0,
 [begin
   [ __1 ]
  end | __Stack].

-compile({inline,yeccpars2_242_/1}).
-file("src/sql92_parser.yrl", 189).
yeccpars2_242_(__Stack0) ->
 [__1 | __Stack] = __Stack0,
 [begin
   # order { key = __1 , sort = asc }
  end | __Stack].

-compile({inline,yeccpars2_244_/1}).
-file("src/sql92_parser.yrl", 154).
yeccpars2_244_(__Stack0) ->
 [__1 | __Stack] = __Stack0,
 [begin
   # key { name = value_of ( __1 ) , alias = value_of ( __1 ) }
  end | __Stack].

-compile({inline,yeccpars2_246_/1}).
-file("src/sql92_parser.yrl", 188).
yeccpars2_246_(__Stack0) ->
 [__2,__1 | __Stack] = __Stack0,
 [begin
   # order { key = __1 , sort = __2 }
  end | __Stack].

-compile({inline,yeccpars2_247_/1}).
-file("src/sql92_parser.yrl", 194).
yeccpars2_247_(__Stack0) ->
 [__1 | __Stack] = __Stack0,
 [begin
   asc
  end | __Stack].

-compile({inline,yeccpars2_248_/1}).
-file("src/sql92_parser.yrl", 195).
yeccpars2_248_(__Stack0) ->
 [__1 | __Stack] = __Stack0,
 [begin
   desc
  end | __Stack].

-compile({inline,yeccpars2_250_/1}).
-file("src/sql92_parser.yrl", 185).
yeccpars2_250_(__Stack0) ->
 [__3,__2,__1 | __Stack] = __Stack0,
 [begin
   [ __1 | __3 ]
  end | __Stack].

-compile({inline,yeccpars2_251_/1}).
-file("src/sql92_parser.yrl", 207).
yeccpars2_251_(__Stack0) ->
 [begin
   undefined
  end | __Stack0].

-compile({inline,yeccpars2_254_/1}).
-file("src/sql92_parser.yrl", 205).
yeccpars2_254_(__Stack0) ->
 [__1 | __Stack] = __Stack0,
 [begin
   [ __1 ]
  end | __Stack].

-compile({inline,yeccpars2_255_/1}).
-file("src/sql92_parser.yrl", 200).
yeccpars2_255_(__Stack0) ->
 [__3,__2,__1 | __Stack] = __Stack0,
 [begin
   __3
  end | __Stack].

-compile({inline,yeccpars2_256_/1}).
-file("src/sql92_parser.yrl", 204).
yeccpars2_256_(__Stack0) ->
 [__1 | __Stack] = __Stack0,
 [begin
   [ value_of ( __1 ) ]
  end | __Stack].

-compile({inline,yeccpars2_258_/1}).
-file("src/sql92_parser.yrl", 202).
yeccpars2_258_(__Stack0) ->
 [__3,__2,__1 | __Stack] = __Stack0,
 [begin
   [ value_of ( __1 ) | __3 ]
  end | __Stack].

-compile({inline,yeccpars2_260_/1}).
-file("src/sql92_parser.yrl", 203).
yeccpars2_260_(__Stack0) ->
 [__3,__2,__1 | __Stack] = __Stack0,
 [begin
   [ __1 | __3 ]
  end | __Stack].

-compile({inline,yeccpars2_261_/1}).
-file("src/sql92_parser.yrl", 210).
yeccpars2_261_(__Stack0) ->
 [begin
   # { }
  end | __Stack0].

-compile({inline,yeccpars2_263_/1}).
-file("src/sql92_parser.yrl", 208).
yeccpars2_263_(__Stack0) ->
 [__2,__1 | __Stack] = __Stack0,
 [begin
   { having , __2 }
  end | __Stack].

-compile({inline,yeccpars2_264_/1}).
-file("src/sql92_parser.yrl", 163).
yeccpars2_264_(__Stack0) ->
 [__5,__4,__3,__2,__1 | __Stack] = __Stack0,
 [begin
   # select { conditions = __1 , order = __2 , group = __3 , limit = map_key ( limit , __5 ) , offset = map_key ( offset , __5 ) }
  end | __Stack].

-compile({inline,yeccpars2_266_/1}).
-file("src/sql92_parser.yrl", 59).
yeccpars2_266_(__Stack0) ->
 [begin
   undefined
  end | __Stack0].

-compile({inline,yeccpars2_269_/1}).
-file("src/sql92_parser.yrl", 148).
yeccpars2_269_(__Stack0) ->
 [__1 | __Stack] = __Stack0,
 [begin
   [ __1 ]
  end | __Stack].

-compile({inline,yeccpars2_272_/1}).
-file("src/sql92_parser.yrl", 56).
yeccpars2_272_(__Stack0) ->
 [__5,__4,__3,__2,__1 | __Stack] = __Stack0,
 [begin
   # insert { table = __3 , values = [ __5 ] }
  end | __Stack].

-compile({inline,yeccpars2_273_/1}).
-file("src/sql92_parser.yrl", 72).
yeccpars2_273_(__Stack0) ->
 [__1 | __Stack] = __Stack0,
 [begin
   [ __1 ]
  end | __Stack].

-compile({inline,yeccpars2_275_/1}).
-file("src/sql92_parser.yrl", 71).
yeccpars2_275_(__Stack0) ->
 [__3,__2,__1 | __Stack] = __Stack0,
 [begin
   [ __1 | __3 ]
  end | __Stack].

-compile({inline,yeccpars2_277_/1}).
-file("src/sql92_parser.yrl", 60).
yeccpars2_277_(__Stack0) ->
 [__3,__2,__1 | __Stack] = __Stack0,
 [begin
   __2
  end | __Stack].

-compile({inline,yeccpars2_279_/1}).
-file("src/sql92_parser.yrl", 147).
yeccpars2_279_(__Stack0) ->
 [__3,__2,__1 | __Stack] = __Stack0,
 [begin
   [ __1 | __3 ]
  end | __Stack].

-compile({inline,yeccpars2_280_/1}).
-file("src/sql92_parser.yrl", 57).
yeccpars2_280_(__Stack0) ->
 [__5,__4,__3,__2,__1 | __Stack] = __Stack0,
 [begin
   # insert { table = __3 , values = zip_insert_values ( __4 , __5 ) }
  end | __Stack].

-compile({inline,yeccpars2_283_/1}).
-file("src/sql92_parser.yrl", 63).
yeccpars2_283_(__Stack0) ->
 [__2,__1 | __Stack] = __Stack0,
 [begin
   __2
  end | __Stack].

-compile({inline,yeccpars2_284_/1}).
-file("src/sql92_parser.yrl", 67).
yeccpars2_284_(__Stack0) ->
 [__1 | __Stack] = __Stack0,
 [begin
   [ __1 ]
  end | __Stack].

-compile({inline,yeccpars2_287_/1}).
-file("src/sql92_parser.yrl", 75).
yeccpars2_287_(__Stack0) ->
 [__1 | __Stack] = __Stack0,
 [begin
   [ __1 ]
  end | __Stack].

-compile({inline,yeccpars2_289_/1}).
-file("src/sql92_parser.yrl", 74).
yeccpars2_289_(__Stack0) ->
 [__3,__2,__1 | __Stack] = __Stack0,
 [begin
   [ __1 | __3 ]
  end | __Stack].

-compile({inline,yeccpars2_290_/1}).
-file("src/sql92_parser.yrl", 69).
yeccpars2_290_(__Stack0) ->
 [__3,__2,__1 | __Stack] = __Stack0,
 [begin
   __2
  end | __Stack].

-compile({inline,yeccpars2_292_/1}).
-file("src/sql92_parser.yrl", 66).
yeccpars2_292_(__Stack0) ->
 [__3,__2,__1 | __Stack] = __Stack0,
 [begin
   [ __1 | __3 ]
  end | __Stack].

-compile({inline,yeccpars2_293_/1}).
-file("src/sql92_parser.yrl", 99).
yeccpars2_293_(__Stack0) ->
 [__2,__1 | __Stack] = __Stack0,
 [begin
   # describe { table = __2 }
  end | __Stack].

-compile({inline,yeccpars2_295_/1}).
-file("src/sql92_parser.yrl", 176).
yeccpars2_295_(__Stack0) ->
 [begin
   undefined
  end | __Stack0].

-compile({inline,yeccpars2_296_/1}).
-file("src/sql92_parser.yrl", 52).
yeccpars2_296_(__Stack0) ->
 [__4,__3,__2,__1 | __Stack] = __Stack0,
 [begin
   # delete { table = __3 , conditions = __4 }
  end | __Stack].

-compile({inline,yeccpars2_297_/1}).
-file("src/sql92_parser.yrl", 29).
yeccpars2_297_(__Stack0) ->
 [__2,__1 | __Stack] = __Stack0,
 [begin
   [ __1 ]
  end | __Stack].

-compile({inline,yeccpars2_298_/1}).
-file("src/sql92_parser.yrl", 28).
yeccpars2_298_(__Stack0) ->
 [__3,__2,__1 | __Stack] = __Stack0,
 [begin
   [ __1 | __3 ]
  end | __Stack].


-file("src/sql92_parser.yrl", 335).
