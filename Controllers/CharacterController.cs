using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using dotnet_rpg.Models;
using System.Collections.Generic;
using System.Linq;
using System;
using dotnet_rpg.Services.CharacterService;
using System.Threading.Tasks;
using dotnet_rpg.Dtos.Character;
using Microsoft.AspNetCore.Authorization;
using System.Security.Claims;

namespace dotnet_rpg.Controllers
{
    [Authorize]
    [ApiController]
    [Route("[controller]")]
    public class CharacterController: ControllerBase
    {

        private readonly ILogger<CharacterController> _logger;

        private readonly ICharacterService _characterService;
        public CharacterController(ILogger<CharacterController> logger, ICharacterService characterService)
        {
            _logger = logger;
            _characterService = characterService;
        }

        //[AllowAnonymous]
        [HttpGet("GetAll")]
        public async Task<IActionResult> Get()
        {
            ServiceResponse<List<GetCharacterDto>> response = null;
            
            try{
                var user = User.Claims.FirstOrDefault(c => c.Type == ClaimTypes.NameIdentifier).Value;
                int id =1;
                if(user != null){
                    id = int.Parse(user);
                }
                Console.WriteLine("user id is "+id);
                var Message= $"Character visited at {DateTime.UtcNow.ToLongTimeString()}";
               // _logger.LogInformation(Message);
                Console.WriteLine(Message);
                 response = await _characterService.GetAllCharacters(id);
                 return Ok(response);
            }catch(Exception ex){
               // response.Message = ex.Message;
               Console.WriteLine(ex.Message);
                return BadRequest(response);
            }
           
        }

        // [Route("GetOne")]
        [HttpGet("{id}")]
        public async Task<IActionResult> GetSingle(int id)
        {
            return Ok(await _characterService.GetCharacterById(id));
        }

        [HttpPost]
        public async Task<IActionResult> AddCharacter(AddCharacterDto newCharacter)
        {
            return Ok(await _characterService.AddCharacter(newCharacter)); 
        }

        [HttpPut]
        public async Task<IActionResult> UpdateCharacter(UpdateCharacterDto updateCharacter)
        {
            ServiceResponse<GetCharacterDto> response = await _characterService.UpdateCharacter(updateCharacter);
            if (response.Data == null){
                return NotFound(response);
            }
            return Ok(response); 
        }
        [HttpDelete("{id}")]
        public async Task<IActionResult> Delete(int id)
        {
            ServiceResponse<List<GetCharacterDto>> response = await _characterService.DeleteCharacter(id);
            if(response.Success == false){
                return NotFound(response);
            }
            return Ok(response);
        }
    }
}