package infore.SDE.ReduceFunctions;

import infore.SDE.synopses.CM;


public class JoinEstimationFunction extends ReduceFunction {

    public JoinEstimationFunction(int nOfP, int count, String[] parameters, int syn,int rq) {
        super(nOfP, count, parameters, syn,rq);

    }


    @Override
    public Object reduce() {
        int n = Integer.parseInt(this.parameters[0]);
        if(indexEstimations.size()!= n){
            System.out.println("size -> " +  indexEstimations.size() + " n -> " + n);
            return null;
        }
        int i =0;
        long[][] tmp = null;
        for (Object cm : indexEstimations.values()){
            CM tcm =(CM)cm;
            if ( i ==0){
                tmp = tcm.getTable();
                i++;
            }
            else{
                for (int ik = 0; ik <tmp.length; i++) {
                    for (int jk = 0; jk < tmp[i].length; jk++) {
                        tmp[ik][jk] = tmp[ik][jk] * tcm.getTable()[ik][jk];
                    }
                }

            }
        }
        long sum=0;
        long min = -1;
        for (int ik = 0; ik <tmp.length; i++) {
            for (int jk = 0; jk < tmp[i].length; jk++) {
                sum = sum+ tmp[ik][jk];
            }
            if(ik == 0){
                min = sum;
            }else{
                if(sum < min){
                    min =sum;
                }
            }
            sum =0;
        }
    return ""+min;
}

}
