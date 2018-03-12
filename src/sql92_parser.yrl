%% @author Oleg Smirnov <oleg.smirnov@gmail.com>
%% Derived under MIT license from https://github.com/master/mongosql
%% @doc A syntax of a subset of SQL-92

Nonterminals
assign_commalist assign atom atom_commalist between_pred column 
column_commalist column_ref column_ref_commalist comparsion_pred 
delete_stmt from_clause in_pred opt_column_commalist
scalar_exp_list insert_stmt like_pred literal opt_order_by_clause
manipulative_stmt opt_group_by_clause sort_spec_commalist
sort_spec sort_key ordering_spec opt_having_clause opt_where_clause
predicate scalar_exp scalar_exp_commalist 
search_cond select_stmt sql sql_list tname table_name table_exp
table_ref table_ref_commalist test_for_null_pred update_stmt 
values_or_select_stmt where_clause opt_limit_offset_clause
transaction_stmt selection_list table_column variable
insert_row_list insert_row insert_set_pair_list describe_stmt 
show_stmt show_type set_stmt set_assign set_assign_list set_value set_target 
opt_full opt_from opt_condition selection_item cast_expr selection_item_alias 
table_ref_alias opt_collate opt_charset use_stmt.

Terminals '-' '+' '*' '/' '(' ')' ',' ';' '=' '.' '<' '>' '<=' '>=' '=<' '=>' '!=' '<>'
describe show delete insert select update from where into values null not in
like between or and group by having is set offset limit
begin rollback commit order asc desc string name integer as var full use
tables databases variables fields index create table collation cast collate character.

Rootsymbol sql_list.

%% Top level rules

sql_list -> sql ';' sql_list : ['$1' | '$3'].
sql_list -> sql ';' : ['$1'].
sql_list -> sql : ['$1'].

sql -> manipulative_stmt : '$1'.

manipulative_stmt -> transaction_stmt : '$1'.
manipulative_stmt -> delete_stmt : '$1'.
manipulative_stmt -> insert_stmt : '$1'.
manipulative_stmt -> select_stmt : '$1'.
manipulative_stmt -> update_stmt : '$1'.
manipulative_stmt -> describe_stmt : '$1'.
manipulative_stmt -> show_stmt : '$1'.
manipulative_stmt -> set_stmt : '$1'.
manipulative_stmt -> use_stmt : '$1'.

%% Transactions

transaction_stmt -> begin : 'begin'.
transaction_stmt -> rollback : 'rollback'.
transaction_stmt -> commit : 'commit'.

%% DELETE

delete_stmt -> delete from table_name opt_where_clause : #delete{table='$3', conditions='$4'}.

%% INSERT statement

insert_stmt -> insert into table_name set insert_set_pair_list : #insert{table='$3', values=['$5']}.
insert_stmt -> insert into table_name opt_column_commalist values_or_select_stmt : #insert{table='$3', values=zip_insert_values('$4', '$5')}.

opt_column_commalist -> '$empty' : undefined.
opt_column_commalist -> '(' column_commalist ')' : '$2'.
opt_column_commalist -> column_commalist : '$1'.

values_or_select_stmt -> values insert_row_list : '$2'.
values_or_select_stmt -> select_stmt : '$1'.

insert_row_list -> insert_row ',' insert_row_list : ['$1' | '$3'].
insert_row_list -> insert_row : ['$1'].

insert_row -> '(' scalar_exp_list ')' : '$2'.

insert_set_pair_list -> assign ',' insert_set_pair_list : ['$1' | '$3'].
insert_set_pair_list -> assign : ['$1'].

scalar_exp_list -> scalar_exp ',' scalar_exp_list : ['$1' | '$3'].
scalar_exp_list -> scalar_exp : ['$1'].

atom -> literal : '$1'.

%% SELECT statement
% select_query <- select_limit / select_order / select_group / select_where / select_from / select_simple ~;

select_stmt -> select selection_list from_clause table_exp : '$4'#select{params='$2', tables='$3'}.
select_stmt -> select selection_list opt_limit_offset_clause: #select{params='$2', limit=map_key(limit, '$3')}.

selection_list -> selection_item_alias ',' selection_list : ['$1'|'$3'].
selection_list -> selection_item_alias : ['$1'].

selection_item_alias -> selection_item 'as' name : apply_alias('$1', value_of('$3')).
selection_item_alias -> selection_item : '$1'.

selection_item -> '(' select_stmt ')' : #subquery{subquery='$2'}.
selection_item -> scalar_exp opt_collate : '$1'.

opt_collate -> collate name : {collate, value_of('$2')}.
opt_collate -> '$empty' : undefined.

%% DESCRIBE

describe_stmt -> describe table_name : #describe{table='$2'}.
show_stmt -> show opt_full create table name: #show{type=create_table, full='$2', from=value_of('$5')}.
show_stmt -> show opt_full show_type opt_from opt_condition: #show{type='$3', full='$2', from='$4', conditions='$5'}.
show_type -> tables : tables.
show_type -> fields : fields.
show_type -> index : index.
show_type -> databases : databases.
show_type -> variables : variables.
show_type -> collation : collation.
opt_full -> full : true.
opt_full -> '$empty' : false.
opt_from -> from name : value_of('$2').
opt_from -> '$empty' : undefined.
opt_condition -> like literal : {like, '$2'}.
opt_condition -> where_clause : '$1'.
opt_condition -> '$empty' : undefined.

set_stmt -> set set_assign_list : #system_set{query='$2'}.
set_assign_list -> set_assign ',' set_assign_list : ['$1' | '$3'].
set_assign_list -> set_assign : ['$1'].
set_assign -> set_target '=' scalar_exp : {'$1', '$3'}.
set_assign -> set_target set_value : {'$1', '$2'}.
set_target -> variable : '$1'.
set_target -> name : #variable{name=value_of('$1'), scope=session}.
set_value -> name : #value{value=value_of('$1')}.
set_value -> literal : #value{value='$1'}.

use_stmt -> use name : {use, value_of('$2')}.

%% UPDATE

update_stmt -> update table_name set assign_commalist opt_where_clause : {update, '$2', '$4', '$5'}.

assign_commalist -> assign ',' assign_commalist : ['$1' | '$3'].
assign_commalist -> assign : ['$1'].

assign -> name '.' column '=' scalar_exp : #set{key='$3', value='$5'}.
assign -> column '=' scalar_exp : #set{key='$1', value='$3'}.

%% Base tables
tname -> name '.' name : {element(1,'$1'),element(2,'$1'),{tolower(value_of('$1')),tolower(value_of('$3'))}}.
tname -> name '.' tables : {element(1,'$1'),element(2,'$1'),{tolower(value_of('$1')),<<"tables">>}}.
tname -> name : {element(1,'$1'),element(2,'$1'), tolower(value_of('$1'))}.

table_name -> tname 'as' name : #table{name=value_of('$1'), alias=value_of('$3')}.
table_name -> tname name 			: #table{name=value_of('$1'), alias=value_of('$2')}.
table_name -> tname 					: #table{name=value_of('$1'), alias=case value_of('$1') of {_,A} -> A; A -> A end}.

column_commalist -> column ',' column_commalist : ['$1' | '$3'].
column_commalist -> column : ['$1'].

column_ref -> name '.' '*' : #all{table=value_of('$1')}.
column_ref -> table_column : '$1'.

table_column -> name '.' name : #key{name=value_of('$3'), alias=value_of('$3'), table=value_of('$1')}.
table_column -> name 					: #key{name=value_of('$1'), alias=value_of('$1')}.

% -record(select, {params, tables, conditions, group, order, limit, offset}).
%% Table expressions
table_exp -> 
	opt_where_clause 
	opt_order_by_clause
	opt_group_by_clause 
	opt_having_clause 
	opt_limit_offset_clause : #select{conditions='$1', order='$2', group='$3', limit=map_key(limit, '$5'), offset=map_key(offset, '$5')}.

from_clause -> from table_ref_commalist : '$2'.

table_ref_commalist -> table_ref_alias ',' table_ref_commalist : ['$1' | '$3'].
table_ref_commalist -> table_ref_alias : ['$1'].

table_ref_alias -> table_ref 'as' name : apply_alias('$1', value_of('$3')).
table_ref_alias -> table_ref : '$1'.

table_ref -> '(' select_stmt ')': #subquery{subquery='$2'}.
table_ref -> table_name : '$1'.

opt_where_clause -> '$empty' : undefined.
opt_where_clause -> where_clause : '$1'.

where_clause -> where search_cond : '$2'.


opt_order_by_clause -> '$empty' : undefined.
opt_order_by_clause -> order by sort_spec_commalist : '$3'.

sort_spec_commalist -> sort_spec ',' sort_spec_commalist : ['$1' | '$3'].
sort_spec_commalist -> sort_spec : ['$1'].

sort_spec -> sort_key ordering_spec : #order{key='$1', sort='$2'}.
sort_spec -> sort_key 							: #order{key='$1', sort=asc}.

sort_key -> table_column : '$1'#key.name.
sort_key -> literal : '$1'.

ordering_spec -> asc : asc.
ordering_spec -> desc : desc.



opt_group_by_clause -> '$empty' : undefined.
opt_group_by_clause -> group by column_ref_commalist : '$3'.

column_ref_commalist -> name ',' column_ref_commalist : [value_of('$1') | '$3'].
column_ref_commalist -> literal ',' column_ref_commalist : ['$1' | '$3'].
column_ref_commalist -> name : [value_of('$1')].
column_ref_commalist -> literal : ['$1'].

opt_having_clause -> '$empty' : undefined.
opt_having_clause -> having search_cond : {having, '$2'}.

opt_limit_offset_clause -> '$empty' : #{}.
opt_limit_offset_clause -> limit literal offset literal : #{limit => '$2', offset => '$4'}.
opt_limit_offset_clause -> limit literal ',' literal : #{offset => '$2', limit => '$4'}.
opt_limit_offset_clause -> limit literal : #{limit => '$2'}.
opt_limit_offset_clause -> offset literal : #{offset => '$2'}.


%% Search conditions

search_cond -> search_cond or search_cond : #condition{nexo=nexo_or, op1='$1', op2='$3'}.
search_cond -> search_cond and search_cond : #condition{nexo=nexo_and, op1='$1', op2='$3'}.
search_cond -> not search_cond : {'not', '$2'}.
search_cond -> '(' search_cond ')' : '$2'.
search_cond -> predicate : '$1'.

predicate -> comparsion_pred : '$1'.
predicate -> between_pred : '$1'.
predicate -> like_pred : '$1'.
predicate -> test_for_null_pred : '$1'.
predicate -> in_pred : '$1'.

% comparsion_pred -> scalar_exp comp scalar_exp : {value_of('$2'), '$1', '$3'}.
% comparsion_pred -> scalar_exp comp '(' select_stmt ')' : {value_of('$2'), '$1', '$4'}.
comparsion_pred -> scalar_exp '=' scalar_exp : #condition{nexo=eq, op1='$1', op2='$3'}.
comparsion_pred -> scalar_exp '>' scalar_exp : #condition{nexo=gt, op1='$1', op2='$3'}.
comparsion_pred -> scalar_exp '<' scalar_exp : #condition{nexo=lt, op1='$1', op2='$3'}.
comparsion_pred -> scalar_exp '>=' scalar_exp : #condition{nexo=gte, op1='$1', op2='$3'}.
comparsion_pred -> scalar_exp '=>' scalar_exp : #condition{nexo=gte, op1='$1', op2='$3'}.
comparsion_pred -> scalar_exp '<=' scalar_exp : #condition{nexo=lte, op1='$1', op2='$3'}.
comparsion_pred -> scalar_exp '=<' scalar_exp : #condition{nexo=lte, op1='$1', op2='$3'}.
comparsion_pred -> scalar_exp '!=' scalar_exp : #condition{nexo=neq, op1='$1', op2='$3'}.
comparsion_pred -> scalar_exp '<>' scalar_exp : #condition{nexo=neq, op1='$1', op2='$3'}.

between_pred -> scalar_exp not between scalar_exp and scalar_exp : #condition{nexo=not_between, op1='$1', op2={'$4', '$6'}}.
between_pred -> scalar_exp between scalar_exp and scalar_exp 		 : #condition{nexo=between,     op1='$1', op2={'$3', '$5'}}.

like_pred -> scalar_exp not like atom : #condition{nexo=not_like, op1='$1', op2='$4'}.
like_pred -> scalar_exp like atom 		: #condition{nexo=like, 		op1='$1', op2='$3'}.

test_for_null_pred -> column_ref is not scalar_exp : #condition{nexo=is_not, 	op1='$1', op2='$4'}.
test_for_null_pred -> column_ref is scalar_exp 		 : #condition{nexo=is, 			op1='$1', op2='$3'}.

in_pred -> scalar_exp not in '(' select_stmt ')' 		: #condition{nexo=not_in, op1='$1',	op2=#subquery{subquery='$5'}}.
in_pred -> scalar_exp in '(' select_stmt ')' 				: #condition{nexo=in, 		op1='$1',	op2=#subquery{subquery='$4'}}.
in_pred -> scalar_exp not in '(' atom_commalist ')' : #condition{nexo=not_in, op1='$1',	op2=#subquery{subquery='$5'}}.
in_pred -> scalar_exp in '(' atom_commalist ')' 		: #condition{nexo=in, 		op1='$1',	op2=#subquery{subquery='$4'}}.

atom_commalist -> atom ',' atom_commalist : ['$1' | '$3'].
atom_commalist -> atom : ['$1'].

%% Scalar expressions
scalar_exp -> cast_expr : '$1'.
scalar_exp -> name '(' scalar_exp_commalist ')' : #function{name=value_of('$1'), params='$3'}.
scalar_exp -> scalar_exp '+' scalar_exp : #operation{type= <<"+">>, op1='$1', op2='$3'}.
scalar_exp -> scalar_exp '-' scalar_exp : #operation{type= <<"-">>, op1='$1', op2='$3'}.
scalar_exp -> scalar_exp '*' scalar_exp : #operation{type= <<"*">>, op1='$1', op2='$3'}.
scalar_exp -> scalar_exp '/' scalar_exp : #operation{type= <<"/">>, op1='$1', op2='$3'}.
scalar_exp -> '(' scalar_exp ')' : '$2'.
scalar_exp -> '*' : #all{}.
% scalar_exp -> atom 'as' name : #value{value='$1', name=value_of('$3')}.
scalar_exp -> column_ref : '$1'.
scalar_exp -> atom : #value{value='$1'}.
scalar_exp -> null : #value{value=undefined}.
scalar_exp -> variable : '$1'.

variable -> var : #variable{name=value_of('$1'), scope=local}.

cast_expr -> cast '(' scalar_exp 'as' name '(' integer ')' opt_charset ')' : #function{name = <<"cast">>, params=['$3', {value_of('$5'), value_of('$7')}]}.
cast_expr -> cast '(' scalar_exp 'as' name opt_charset ')' : #function{name = <<"cast">>, params=['$3', value_of('$5')]}.

opt_charset -> character set name : value_of('$3').
opt_charset -> '$empty' : undefined.

scalar_exp_commalist -> scalar_exp ',' scalar_exp_commalist : ['$1'|'$3'].
scalar_exp_commalist -> scalar_exp : ['$1'].
scalar_exp_commalist -> '$empty' : [].


column -> name : value_of('$1').
literal -> integer : value_of('$1').
literal -> string : value_of('$1').


Erlang code.
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