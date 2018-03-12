%% @author Oleg Smirnov <oleg.smirnov@gmail.com>
%% Derived under MIT license from https://github.com/master/mongosql
%% @doc A grammar of a subset of SQL-92

Definitions.

D   = [0-9]
V   = [A-Za-z_][A-Za-z0-9_\.]*
L   = [A-Za-z_][A-Za-z0-9_]*
WS  = ([\000-\s]|%.*)
C   = (<=|>=|>|<>|=<|=>|<|<>|!=)
P   = [-+*/(),;=.]

Rules.

{C}     : {token, {atom(TokenChars), TokenLine}}.
{D}+    : {token, {integer, TokenLine, integer(TokenChars)}}.
{P}     : {token, {atom(TokenChars), TokenLine}}.
'(\\\^.|\\.|('')|[^'])*' : S = string_gen(strip(TokenChars, TokenLen)),
 		      {token, {string, TokenLine, bitstring(S)}}.
"(\\\^.|\\.|("")|[^"])*" : S = string_gen(strip(TokenChars, TokenLen)),
 		      {token, {string, TokenLine, bitstring(S)}}.
`(\\\^.|\\.|[^`])*` : S = string_gen(strip(TokenChars, TokenLen)),
          {token, {name, TokenLine, bitstring(S)}}.
@@{V}+  : {token, {var, TokenLine, variable(TokenChars)}}.
{L}+    : check_reserved(TokenChars, TokenLine).
{WS}+   : skip_token.

Erlang code.

check_reserved(TokenChars, TokenLine) ->
  R = atom(string_gen(TokenChars)),
  case reserved_word(R) of
    true -> {token, {R, TokenLine}};
    false -> {token, {name, TokenLine, bitstring(TokenChars)}}
  end.

atom(TokenChars) -> list_to_atom(string:to_lower(TokenChars)).
integer(TokenChars) -> list_to_integer(TokenChars).
bitstring(TokenChars) -> list_to_bitstring(TokenChars).
strip(TokenChars,TokenLen) -> lists:sublist(TokenChars, 2, TokenLen - 2).
variable([_,_|TokenChars]) -> list_to_bitstring(TokenChars).

reserved_word('all') -> true;
reserved_word('and') -> true;
reserved_word('asc') -> true;
reserved_word('begin') -> true;
reserved_word('between') -> true;
reserved_word('by') -> true;
reserved_word('commit') -> true;
reserved_word('delete') -> true;
reserved_word('desc') -> true;
reserved_word('distinct') -> true;
reserved_word('from') -> true;
reserved_word('group') -> true;
reserved_word('having') -> true;
reserved_word('insert') -> true;
reserved_word('in') -> true;
reserved_word('into') -> true;
reserved_word('is') -> true;
reserved_word('like') -> true;
reserved_word('limit') -> true;
reserved_word('not') -> true;
reserved_word('null') -> true;
reserved_word('offset') -> true;
reserved_word('or') -> true;
reserved_word('order') -> true;
reserved_word('rollback') -> true;
reserved_word('select') -> true;
reserved_word('describe') -> true;
reserved_word('show') -> true;
reserved_word('full') -> true;
reserved_word('set') -> true;
reserved_word('update') -> true;
reserved_word('values') -> true;
reserved_word('where') -> true;
reserved_word('as') -> true;
reserved_word('fields') -> true;
reserved_word('index') -> true;
reserved_word('create') -> true;
reserved_word('table') -> true;
reserved_word('tables') -> true;
reserved_word('databases') -> true;
reserved_word('variables') -> true;
reserved_word('collation') -> true;
reserved_word('collate') -> true;
reserved_word('character') -> true;
reserved_word('cast') -> true;
reserved_word('use') -> true;
reserved_word(_) -> false.

string_gen([A,A|Cs]) when A == $'; A == $" -> 
  [A|string_gen(Cs)];
string_gen([$\\|Cs]) ->
    string_escape(Cs);
string_gen([C|Cs]) ->
    [C|string_gen(Cs)];
string_gen([]) -> [].

string_escape([C|Cs]) ->
  case escape_char(C) of
    not_control -> [$\\, C|string_gen(Cs)];
    Control -> [Control|string_gen(Cs)]
  end;
string_escape([]) -> [$\\].

%% https://dev.mysql.com/doc/refman/5.7/en/string-literals.html#character-escape-sequences
escape_char($0) -> $\0;
escape_char($') -> $\';
escape_char($") -> $\";
escape_char($b) -> $\b;
escape_char($n) -> $\n;
escape_char($r) -> $\r;
escape_char($t) -> $\t;
escape_char($Z) -> 10#26;
escape_char($\\) -> $\\;
%% escape_char($%) -> $%; % TODO except like
%% escape_char($_) -> $_; % TODO except like
escape_char(_) -> not_control.
