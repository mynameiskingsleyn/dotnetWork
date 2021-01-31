namespace dotnet_rpg.Models
{
    public class ServiceResponse<T>
    {
        public bool Success {get; set;} = true;

        public string Message{get; set; } = null;

        public T Data {get; set;}
    }
}