The logic of extracting the join condition and select condition is to use a Visitor object to split AND and extract every singel clause.
For every single clause, I can check whether it has two "." 
since only join condition has two dots, select condition only has one dot and one integer on the other side.
Next step is to store both joins and select conditios to two arraylist for the following invoking.
This should be done by split where expression class.
But this function is not implemented completely since the time limit and heavy work loud recently.

Although I have implemeted all the required operator, the query plan is not complete.
There are some comments in the blank code blocks showing the logic tree I construted.

Another problem is about aliases.
I have tried only modify the places that require table name, but it's a big work probably since my code stucture is not good.
So I tried another method to replace all the aliases in the query and just parse the new query.
But this method is really complicated and I have not done yet, so I just put the code I have wrote in the queryplan class(have a try method).

Since I have only done single operator methods test, the whole end to end test may occur serious issues.
So plz check the comments in the code especially the query plan part. I have a relatively clear idea but not enough time to implement them.

