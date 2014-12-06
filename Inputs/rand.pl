open fpOut, ">", "rand.txt";

for ($i=1;$i<255;$i++) {
        $a = int(rand(100));
        $b = int(rand(100));
        $c = int(rand(100)+1);
        #if ($a != $b) {
                print fpOut "$i\t".($i+1)."\t$c\n";
        #}
}

close fpOut;
