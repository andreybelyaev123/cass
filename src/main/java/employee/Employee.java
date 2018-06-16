package employee;

import java.util.UUID;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;



@Table(keyspace = "employee_schema", name = "employees",
readConsistency = "LOCAL_QUORUM",
writeConsistency = "LOCAL_QUORUM",
caseSensitiveKeyspace = false,
caseSensitiveTable = false)
public class Employee {
	
	@Column(name = "id")
	private UUID id;
	@PartitionKey
	@Column(name = "position")
	private String position;
	@Column(name = "last_name")
	private String lastName;
	@Column(name = "first_name")
	private String firstName;
	@Column(name = "salary")
	private Integer salary;
	@Column(name = "seniority_level")
	private String seniorityLevel;
	@Column(name = "employment_type")
	private String employmentType;
	
	public Employee() {}
	
	public void setId(UUID id) {
		this.id = id;
	}
	public UUID getId() {
		return this.id;
	}
	public void setPosition(String position) {
		this.position = position;
	}
	public String getPosition() {
		return this.position;
	}
	public void setLastName(String lastName) {
		this.lastName = lastName;
	}
	public String getLastName() {
		return this.lastName;
	}
	public void setFirstName(String firstName) {
		this.firstName = firstName;
	}
	public String getFirstName() {
		return this.firstName;
	}
	public void setSalary(Integer salary) {
		this.salary = salary;
	}
	public Integer getSalary() {
		return this.salary;
	}
	public void setSeniorityLevel(String seniorityLevel) {
		this.seniorityLevel = seniorityLevel;
	}
	public String getSeniorityLevel() {
		return this.seniorityLevel;
	}
	public void setEmploymentType(String employmentType) {
		this.employmentType = employmentType;
	}
	public String getEmploymentType() {
		return this.employmentType;
	}
}

/*
@Table(keyspace = "samlib", name = "samlib_update",
readConsistency = "LOCAL_QUORUM",
writeConsistency = "LOCAL_QUORUM",
caseSensitiveKeyspace = false,
caseSensitiveTable = false)
public class SamlibUpdate {
	@Computed("uuid")
	@Column(name = "id")
	private UUID id;
	@PartitionKey
	@Column(name = "author_name")
	private String authorName;
	@Column(name = "author_tag")
	private String authorTag;
	@Column(name = "last_update")
	private LocalDate lastUpdate;
	@Column(name = "web_page")
	private String webPage;
	@Frozen
	@Column(name = "updates")
	private Set<AuthorUpdate> updates;
	
	public SamlibUpdate() {}
	
	public SamlibUpdate setId(UUID id) {
		this.id = id;
		return this;
	}
	public SamlibUpdate setAuthorName(String authorName) {
		this.authorName = authorName;
		return this;
	}
	public SamlibUpdate setAuthorTag(String authorTag) {
		this.authorTag = authorTag;
		return this;
	}
	public SamlibUpdate setLastUpdate(LocalDate lastUpdate) {
		this.lastUpdate = lastUpdate;
		return this;
	}
	public SamlibUpdate setWebPage(String webPage) {
		this.webPage = webPage;
		return this;
	}
	public SamlibUpdate setUpdates(Set<AuthorUpdate> updates) {
		this.updates = updates;
		return this;
	}
	public SamlibUpdate addUpdate(AuthorUpdate update) {
		this.updates.add(update);
		return this;
	}
	public SamlibUpdate removeUpdate(AuthorUpdate update) {
		this.updates.remove(update);
		return this;
	}
	public UUID getId() {
		return id;
	}
	public String getAuthorName() {
		return authorName;
	}
	public String getAuthorTag() {
		return authorTag;
	}
	public LocalDate getLastUpdate() {
		return lastUpdate;
	}
	public String getWebPage() {
		return webPage;
	}
	public Set<AuthorUpdate> getUpdates() {
		return updates;
	}
}

 */
