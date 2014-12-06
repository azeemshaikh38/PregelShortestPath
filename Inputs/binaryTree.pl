open fpOut, ">", "binaryTree.txt";

$parentNode = 1;
$childNode = 2;

$maxNode=$ARGV[0];

while ($childNode<$maxNode) {
        $a = int(rand(100)) + 1;
        print fpOut "$parentNode\t$childNode\t$a\n";
        $childNode++;
        $a = int(rand(100)) + 1;
        print fpOut "$parentNode\t$childNode\t$a\n";
        $childNode++;
        $parentNode++;
}

close fpOut;
