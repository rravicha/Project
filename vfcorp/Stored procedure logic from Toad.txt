CREATE OR REPLACE PROCEDURE I2VFCDEV."DELETE_INACTIVE_ROUTINGS" is

V_STYLE_COLOR VARCHAR2(11);

V_DC VARCHAR(4);

V_ERRCDE VARCHAR(40);

TYPE del_rout is TABLE OF DELETED_ROUTING%ROWTYPE;

inactive_routes del_rout;


CURSOR INACTIVE_ROUTING IS 

   SELECT DISTINCT STYLE_COLOR, DC FROM ROUTINGS_DELETE_MATERIAL_DC ;                       
   
   
BEGIN

OPEN INACTIVE_ROUTING;

LOOP 

    FETCH INACTIVE_ROUTING INTO V_STYLE_COLOR, V_DC;
    
    select 1 as deleted_routing_key,l.loc_code,m.style_color,r.RTING_PRIORITY ,
     r.RTING_STATUS ,r.COALITION_KEY ,
     r.UPDATED_DATE , 
     r.UPDATED_BY , 1 as dml_seqnm,L.LOC_CODE ,T.SAP_MAP_KEY 
     BULK COLLECT INTO inactive_routes from 
     routing r, routing_step rs,location l, location_group lg, material m, 
     linked_dc ld, trans_mode t
        where m.style_color = V_STYLE_COLOR
        and l.loc_code = V_DC
        and lg.material_key = m.material_key
        and ld.location_group_key = lg.location_group_key
        and ld.location_key = l.location_key
        and r.routing_material_key = lg.material_key
        and r.location_group_key = ld.location_group_key
        and rs.routing_key = r.routing_key
        and rs.trans_mode_key = t.trans_mode_key
        and rs.oper_location_key = l.location_key;
        
        
        delete  from routing_step where routing_step_key in 
                (select rs.routing_step_key from routing_step rs,routing r, 
                location l, location_group lg, material m, linked_dc ld
                where m.style_color =V_STYLE_COLOR
                and l.loc_code = V_DC
                and lg.material_key = m.material_key
                and ld.location_group_key = lg.location_group_key
                and ld.location_key = l.location_key
                and r.routing_material_key = lg.material_key
                and r.location_group_key = ld.location_group_key
                and rs.routing_key = r.routing_key); 
                
            

    delete from routing where routing_key in 
                (select r.routing_key from routing r, location l, 
                location_group lg, material m, linked_dc ld
                where m.style_color = V_STYLE_COLOR
                and l.loc_code = V_DC
                and lg.material_key = m.material_key
                and ld.location_group_key = lg.location_group_key
                and ld.location_key = l.location_key
                and r.routing_material_key = lg.material_key
                and r.location_group_key = ld.location_group_key);
                


    delete from linked_dc where linked_dc_key in
         (select distinct ld.linked_dc_key from linked_dc ld, location l 
        where l.loc_code = V_DC 
        and ld.location_key = l.location_key 
        and location_group_key in 
            (select distinct lg.location_group_key from location_group lg, 
            material m 
            where location_group_key not in 
                (select distinct lg.location_group_key from routing r, 
                location_group lg, material m
                where lg.location_group_key = r.location_group_key  
                and lg.material_key = m.material_key
                and r.routing_material_key = m.material_key) 
                and lg.material_key =m.material_key
                and m.style_color = V_STYLE_COLOR));
                


    delete from location_group where location_group_key in 
        ( select distinct lg.location_group_key from location_group lg, 
        material m 
        where location_group_key not in 
            (select distinct lg.location_group_key from routing r, 
            location_group lg, material m
            where lg.location_group_key = r.location_group_key  
            and lg.material_key = m.material_key
            and r.routing_material_key = m.material_key)
            and lg.material_key =m.material_key
            and m.style_color = V_STYLE_COLOR);
        
        V_ERRCDE := SQLCODE;
            
        IF V_ERRCDE = 0 THEN
        
        
        FOR i in inactive_routes.First..inactive_routes.Last loop
         
         insert into DELETED_ROUTING values inactive_routes(i);
        end loop;
        commit;
        end if;
    END LOOP;
    

CLOSE INACTIVE_ROUTING;
    
 
END DELETE_INACTIVE_ROUTINGS;
/
