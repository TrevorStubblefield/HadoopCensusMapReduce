package cs455.hadoop;

import java.util.ArrayList;


/**
 * Everything in this file parsed the line based on indices, they are based on the method names.
 */
public class Record {

    public String line, state, summary, logicalRecordNumber, logicalRecordPartNumber, totalNumberOfPartsInRecord;

    public Record(String line){
        this.line = line;
        this.state = line.substring(8,10);
        this.summary = line.substring(10,13);
        this.logicalRecordNumber = line.substring(18,24);
        this.logicalRecordPartNumber = line.substring(24,28);
        this.totalNumberOfPartsInRecord = line.substring(28,32);
    }

    //Q1
    public String getResidencesOwned(){
        String residencesOwned = "";
        if(line.charAt(0) != '2' && this.summary.equals("100") && Integer.parseInt(logicalRecordPartNumber) == 2){
            residencesOwned = line.substring(1803,1812);
        }
        return residencesOwned;
    }

    //Q1
    public String getResidencesRented(){
        String residencesRented = "";
        if(line.charAt(0) != '2' && this.summary.equals("100") && Integer.parseInt(logicalRecordPartNumber) == 2){
            residencesRented = line.substring(1812,1821);
        }
        return residencesRented;
    }

    //Q2
    public String getMaleNeverMarried(){
        String neverMarried = "";
        if(line.charAt(0) != '2' && this.summary.equals("100") && Integer.parseInt(logicalRecordPartNumber) == 1){
            neverMarried = line.substring(4422,4431);
        }
        return neverMarried;
    }

    //Q2
    public String getFemaleNeverMarried(){
        String neverMarried = "";
        if(line.charAt(0) != '2' && this.summary.equals("100") && Integer.parseInt(logicalRecordPartNumber) == 1){
            neverMarried = line.substring(4467,4476);
        }
        return neverMarried;
    }

    //Q2 and Q8
    public String getTotalPopulation(){
        String total = "";
        if(line.charAt(0) != '2' && this.summary.equals("100") && Integer.parseInt(logicalRecordPartNumber) == 1){
            total = line.substring(300,309);
        }
        return total;
    }

    //Q3
    public String getHispanicAgeData(){

        String value = "";

        String malesUnder18 = "";
        String malesBetween19and29 = "";
        String malesBetween30and39 = "";

        String femalesUnder18 = "";
        String femalesBetween19and29 = "";
        String femalesBetween29and39 = "";

        String totalPopulationString = "";

        if(line.charAt(0) != '2' && this.summary.equals("100") && Integer.parseInt(logicalRecordPartNumber) == 1){
            int under18 = 0;
            int between19and29 = 0;
            int between29and39 = 0;
            int totalPopulation = 0;

            //Grabbing all male age data.
            for(int i = 3864; i <= 4134; i+=9){
                if(i < 3981) {
                    under18 += get9CharacterValue(i);
                    totalPopulation += get9CharacterValue(i);
                }
                else if (i >= 3981 && i < 4026){
                    between19and29 += get9CharacterValue(i);
                    totalPopulation += get9CharacterValue(i);
                }
                else if (i >= 4026 && i < 4044){
                    between29and39 += get9CharacterValue(i);
                    totalPopulation += get9CharacterValue(i);
                }
                else{
                    totalPopulation += get9CharacterValue(i);
                }
            }

            malesUnder18 = ""+under18 + " ";
            malesBetween19and29 = ""+between19and29 + " ";
            malesBetween30and39 = ""+between29and39 + " ";

            under18 = 0;
            between19and29 = 0;
            between29and39 = 0;

            //Grabbing all female age data.
            for(int i = 4143; i <= 4413; i+=9){
                if(i < 4260) {
                    under18 += get9CharacterValue(i);
                    totalPopulation += get9CharacterValue(i);
                }
                else if (i >= 4260 && i < 4305){
                    between19and29 += get9CharacterValue(i);
                    totalPopulation += get9CharacterValue(i);
                }
                else if (i >= 4305 && i < 4323){
                    between29and39 += get9CharacterValue(i);
                    totalPopulation += get9CharacterValue(i);
                }
                else {
                    totalPopulation += get9CharacterValue(i);
                }
            }

            femalesUnder18 = ""+under18 + " ";
            femalesBetween19and29 = ""+between19and29 + " ";
            femalesBetween29and39 = ""+between29and39 + " ";
            totalPopulationString = ""+totalPopulation;

        }

        value = malesUnder18+malesBetween19and29+malesBetween30and39+femalesUnder18+femalesBetween19and29+femalesBetween29and39+totalPopulationString;
        return value;
    }

    //Q4
    public String getUrbanVsRural(){
        String value = "";

        String urbanHouses = "";
        String ruralHouses = "";
        String totalHouses = "";

        if(line.charAt(0) != '2' && this.summary.equals("100") && Integer.parseInt(logicalRecordPartNumber) == 2) {

            int u = get9CharacterValue(1821)+get9CharacterValue(1830);
            int r = get9CharacterValue(1839);
            int t = get9CharacterValue(1821)+get9CharacterValue(1830)+get9CharacterValue(1839)+get9CharacterValue(1848);

            urbanHouses = ""+ u + " ";
            ruralHouses = ""+ r + " ";
            totalHouses = ""+ t;
            value = urbanHouses + ruralHouses + totalHouses;
        }

        return value;

    }

    //Q5
    public ArrayList<String> getMedianHouseValue() {
        ArrayList<String> totalHouses = new ArrayList<>();

        if (line.charAt(0) != '2' && this.summary.equals("100") && Integer.parseInt(logicalRecordPartNumber) == 2) {

            for (int i = 2928; i <= 3099; i += 9) {

                int current = get9CharacterValue(i);

                if (i == 2928)
                    totalHouses.add("lessThan$15000 " + current);
                else if (i == 2937)
                    totalHouses.add("$15000to$19999 " + current);
                else if (i == 2946)
                    totalHouses.add("$20000to$24999 " + current);
                else if (i == 2955)
                    totalHouses.add("$25000to$29999 " + current);
                else if (i == 2964)
                    totalHouses.add("$30000to$34999 " + current);
                else if (i == 2973)
                    totalHouses.add("$35000to$39999 " + current);
                else if (i == 2982)
                    totalHouses.add("$40000to$44999 " + current);
                else if (i == 2991)
                    totalHouses.add("$45000to$49999 " + current);
                else if (i == 3000)
                    totalHouses.add("$50000to$59999 " + current);
                else if (i == 3009)
                    totalHouses.add("$60000to$74999 " + current);
                else if (i == 3018)
                    totalHouses.add("$75000to$99999 " + current);
                else if (i == 3027)
                    totalHouses.add("$100000to$124999 " + current);
                else if (i == 3036)
                    totalHouses.add("$125000to$149999 " + current);
                else if (i == 3045)
                    totalHouses.add("$150000to$174999 " + current);
                else if (i == 3054)
                    totalHouses.add("$175000to$199999 " + current);
                else if (i == 3063)
                    totalHouses.add("$200000to$249999 " + current);
                else if (i == 3072)
                    totalHouses.add("$250000to$299999 " + current);
                else if (i == 3081)
                    totalHouses.add("$300000to$399999 " + current);
                else if (i == 3090)
                    totalHouses.add("$400000to499999 " + current);
                else if (i == 3099)
                    totalHouses.add("$500000OrMore " + current);
            }
            return totalHouses;
        }
        return null;
    }

    //Q6
    public ArrayList<String> getMedianRentValue(){

        ArrayList<String> totalHouses = new ArrayList<>();

        if (line.charAt(0) != '2' && this.summary.equals("100") && Integer.parseInt(logicalRecordPartNumber) == 2) {

            for (int i = 3450; i <= 3585; i += 9) {

                int current = get9CharacterValue(i);

                if (i == 3450)
                    totalHouses.add("lessThan$100 " + current);
                else if (i == 3459)
                    totalHouses.add("$100to$149 " + current);
                else if (i == 3468)
                    totalHouses.add("$150to$199 " + current);
                else if (i == 3477)
                    totalHouses.add("$200to$249 " + current);
                else if (i == 3486)
                    totalHouses.add("$250to$299 " + current);
                else if (i == 3495)
                    totalHouses.add("$300to$349 " + current);
                else if (i == 3504)
                    totalHouses.add("$350to$399 " + current);
                else if (i == 3513)
                    totalHouses.add("$400to$449 " + current);
                else if (i == 3522)
                    totalHouses.add("$450to$499 " + current);
                else if (i == 3531)
                    totalHouses.add("$500to$549 " + current);
                else if (i == 3540)
                    totalHouses.add("$550to$599 " + current);
                else if (i == 3549)
                    totalHouses.add("$600to$649 " + current);
                else if (i == 3558)
                    totalHouses.add("$650to$699 " + current);
                else if (i == 3567)
                    totalHouses.add("$700to$749 " + current);
                else if (i == 3576)
                    totalHouses.add("$750to$999 " + current);
                else if (i == 3585)
                    totalHouses.add("$1000OrMore " + current);
            }
            return totalHouses;
        }
        return null;

    }

    //Q7
    public String getNumberOfRooms(){
        String totalRooms = "";

        if (line.charAt(0) != '2' && this.summary.equals("100") && Integer.parseInt(logicalRecordPartNumber) == 2) {

            int totalRoomsInt = 0;
            int totalHouses = 0;

            for (int i = 2388; i <= 2460; i += 9) {

                int current = get9CharacterValue(i);

                //1 Room
                if (i == 2388) {
                    totalRoomsInt += (current);
                    totalHouses += current;
                }
                //2 Rooms
                else if (i == 2397) {
                    totalRoomsInt += (current * 2);
                    totalHouses += current;
                }
                //3 Rooms
                else if (i == 2406) {
                    totalRoomsInt += (current * 3);
                    totalHouses += current;
                }
                //4 Rooms
                else if (i == 2415) {
                    totalRoomsInt += (current * 4);
                    totalHouses += current;
                }
                //5 Rooms
                else if (i == 2424) {
                    totalRoomsInt += (current * 5);
                    totalHouses += current;
                }
                //6 Rooms
                else if (i == 2433) {
                    totalRoomsInt += (current * 6);
                    totalHouses += current;
                }
                //7 Rooms
                else if (i == 2442) {
                    totalRoomsInt += (current * 7);
                    totalHouses += current;
                }
                //8 Rooms
                else if (i == 2451) {
                    totalRoomsInt += (current * 8);
                    totalHouses += current;
                }
                //9 Rooms
                else if (i == 2460) {
                    totalRoomsInt += (current * 9);
                    totalHouses += current;
                }
            }
            totalRooms = ""+totalRoomsInt+" "+totalHouses;
        }
        return totalRooms;
    }

    //Q8
    public String getNumberOfElderly(){
        String output = "";
        if(line.charAt(0) != '2' && this.summary.equals("100") && Integer.parseInt(logicalRecordPartNumber) == 1){
            output += getTotalPopulation() + " ";
            output += line.substring(1065,1074);
        }
        return output;
    }

    //Q9
    public String getNumberOfTenantsOwned() {
        String totalTenants = "";

        if (line.charAt(0) != '2' && this.summary.equals("100") && Integer.parseInt(logicalRecordPartNumber) == 2) {

            int totalTenantsInt = 0;
            int totalHouses = 0;

            for (int i = 2622; i <= 2676; i += 9) {

                int current = get9CharacterValue(i);

                if (i == 2622) {
                    totalTenantsInt += (current);
                    totalHouses += current;
                }
                else if (i == 2631) {
                    totalTenantsInt += (current * 2);
                    totalHouses += current;
                }
                else if (i == 2640) {
                    totalTenantsInt += (current * 3);
                    totalHouses += current;
                }
                else if (i == 2649) {
                    totalTenantsInt += (current * 4);
                    totalHouses += current;
                }
                else if (i == 2658) {
                    totalTenantsInt += (current * 5);
                    totalHouses += current;
                }
                else if (i == 2667) {
                    totalTenantsInt += (current * 6);
                    totalHouses += current;
                }
                else if (i == 2676) {
                    totalTenantsInt += (current * 7);
                    totalHouses += current;
                }
            }
            totalTenants = ""+totalTenantsInt+" "+totalHouses;
        }
        return totalTenants;
    }

    //Q9
    public String getNumberOfTenantsRented() {
        String totalTenants = "";

        if (line.charAt(0) != '2' && this.summary.equals("100") && Integer.parseInt(logicalRecordPartNumber) == 2) {

            int totalTenantsInt = 0;
            int totalHouses = 0;

            for (int i = 2685; i <= 2739; i += 9) {

                int current = get9CharacterValue(i);

                if (i == 2685) {
                    totalTenantsInt += (current);
                    totalHouses += current;
                }
                else if (i == 2694) {
                    totalTenantsInt += (current * 2);
                    totalHouses += current;
                }
                else if (i == 2703) {
                    totalTenantsInt += (current * 3);
                    totalHouses += current;
                }
                else if (i == 2712) {
                    totalTenantsInt += (current * 4);
                    totalHouses += current;
                }
                else if (i == 2721) {
                    totalTenantsInt += (current * 5);
                    totalHouses += current;
                }
                else if (i == 2730) {
                    totalTenantsInt += (current * 6);
                    totalHouses += current;
                }
                else if (i == 2739) {
                    totalTenantsInt += (current * 7);
                    totalHouses += current;
                }
            }
            totalTenants = ""+totalTenantsInt+" "+totalHouses;
        }
        return totalTenants;
    }

    public int get9CharacterValue(int index){
        return Integer.parseInt(line.substring(index,index+9));
    }


}