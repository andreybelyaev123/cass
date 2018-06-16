package employee;

import com.datastax.driver.mapping.Result;  
import com.datastax.driver.mapping.annotations.Accessor;  
import com.datastax.driver.mapping.annotations.Param;  
import com.datastax.driver.mapping.annotations.Query;
import com.google.common.util.concurrent.ListenableFuture;

@Accessor
interface EmployeeAccessor {  
    @Query("SELECT * FROM employee_schema.employees WHERE position=:position")
    ListenableFuture<Result<Employee>> getAllByPosition(@Param("position") String position);
}
