// Package kdb implements encoding and decoding of q ipc message format
package kdb

/*
Kdb+ type name	Kdb+ type number	Encoded type name	C type	Size in bytes	Interface list accessor function
mixed list	0	-	K	-	kK
boolean		1	KB	char	1	kG
guid		2	UU	U	16	kU
byte		4	KG	char	1	kG
short		5	KH	short	2	kH
int			6	KI	int	4	kI
long		7	KJ	int64_t	8	kJ
real		8	KE	float	4	kE
float		9	KF	double	8	kF
char		10	KC	char	1	kC
symbol		11	KS	char*	4 or 8	kS
timestamp	12	KP	int64_t	8	kJ
month		13	KM	int	4	kI
date		14	KD	int	4	kI (days from 2000.01.01)
datetime	15	KZ	double	8	kF (days from 2000.01.01)
timespan	16	KN	int64_t	8	kJ
minute		17	KU	int	4	kI
second		18	KV	int	4	kI
time		19	KT	int	4	kI (milliseconds)
table/flip	98	XT	-	-	x->k
dict/table with primary keys	99	XD	-	-	kK(x)[0] for keys and kK(x)[1] for values
error	-128	-	char*	4 or 8	kS
*/
